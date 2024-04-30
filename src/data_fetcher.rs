// data_fetcher.rs
use anyhow::{Result, Context};
use chrono::{Local, NaiveDate};
use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, Url};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// 定义支持的数据类型
// https://www.okx.com/priapi/v5/broker/public/v2/orderRecord?path=cdn/okex/traderecords&nextMarker=&size=30
#[derive(ValueEnum, Clone)]
pub enum DataType {
    SwapRate,   // 永续资金费率
    AggTrades,  // 归集交易
    Trades,     // 交易历史
}

impl DataType {
    pub fn as_str(&self) -> &str {
        match self {
            DataType::SwapRate => "swaprate",
            DataType::AggTrades => "aggtrades",
            DataType::Trades => "trades",
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DateRecord {
    pub downloaded: bool,
    pub files: Vec<FileRecord>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FileRecord {
    pub file_name: String,
}

pub struct DateFilesFetcher {
    client: Client,
    base_url: Url,
    data_store_path: String,
    records: HashMap<String, DateRecord>,
    last_update: Option<NaiveDate>,
    ignore_undownloaded: bool,
    data_type: DataType,
    today: NaiveDate,
}

impl DateFilesFetcher {
    pub async fn new(data_type: DataType) -> Result<Self> {
        let client = Client::new();
        let base_url = Url::parse("https://www.okx.com/priapi/v5/broker/public/v2/orderRecord").unwrap();
        let today = Local::now().date_naive();
        let mut fetcher = DateFilesFetcher {
            client,
            base_url,
            data_store_path: format!("{}.json", data_type.as_str()),
            records: HashMap::new(),
            last_update: None,
            ignore_undownloaded: false,
            data_type,
            today,
        };

        // 尝试加载本地文件
        if Path::new(&fetcher.data_store_path).exists() {
            // 尝试加载，如果成功则不动，失败则拉取数据
            if let Err(_) = fetcher.load_from_file().await {
                log::info!("无法加载历史文件，重新拉取历史数据....");
                // 如果加载失败，则从网络重新拉取
                fetcher.fetch_and_save().await?;
            } else {
                // 检查数据是否是最新的，如果不是今天的，则更新
                if Some(fetcher.today) != fetcher.last_update {
                    log::info!("历史数据存在更新，拉取更新数据.....");
                    fetcher.fetch_and_save().await?;
                }
            }
        } else {
            // 文件不存在，直接拉取数据
            log::info!("无法加载历史文件，重新拉取历史数据....");
            fetcher.fetch_and_save().await?;
        }
        log::info!("加载数据成功, 一共有 {} 天数据", fetcher.get_records_num());
        Ok(fetcher)
    }

    fn get_records_num(&self) -> usize {
        self.records.len()
    }

    pub fn set_ignore_undownloaded(&mut self) {
        self.ignore_undownloaded = true;
    }

    async fn load_from_file(&mut self) -> Result<()> {
        let mut file = File::open(&self.data_store_path).await.context("Failed to open file")?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).await.context("Failed to read file")?;
        let json_data: serde_json::Value = serde_json::from_str(&contents).context("Failed to parse JSON data")?;
    
        // 解析 last_update
        if let Some(last_update_str) = json_data["last_update"].as_str() {
            self.last_update = Some(NaiveDate::parse_from_str(last_update_str, "%Y-%m-%d")?);
        }
    
        // 解析 dates
        if let Some(dates) = json_data["dates"].as_object() {
            self.records = dates.iter().map(|(date, record)| {
                let downloaded = record["downloaded"].as_bool().unwrap_or(false);
                let files = record["files"].as_array().unwrap_or(&vec![]).iter().map(|file| {
                    FileRecord {
                        file_name: file.as_str().unwrap().to_string(),
                    }
                }).collect();
    
                (date.clone(), DateRecord {
                    downloaded,
                    files,
                })
            }).collect();
        }

        Ok(())
    }
    

    async fn fetch_and_save(&mut self) -> Result<()> {
        // Assume fetch_dates now takes an optional NaiveDate and fetches dates after this date
        self.records = self.fetch_dates(self.last_update).await?;
        self.last_update = Some(self.today); // Update the last update time to today
        self.save_to_file().await
    }

    async fn fetch_dates(&self, last_known_date: Option<NaiveDate>) -> Result<HashMap<String, DateRecord>> {
        let mut records = HashMap::new();
        let mut next_marker: Option<String> = None;
        let mut continue_fetching = true;
    
        let pb = ProgressBar::new(100);
        pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} Fetching dates: {msg}")?);
    
        while continue_fetching {
            let mut url = self.base_url.clone();
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            url.query_pairs_mut().append_pair("path", &format!("cdn/okex/traderecords/{}/daily", self.data_type.as_str()));
            url.query_pairs_mut().append_pair("size", "100");
            url.query_pairs_mut().append_pair("t", &timestamp.to_string());
            if let Some(marker) = &next_marker {
                url.query_pairs_mut().append_pair("nextMarker", marker);
            }
    
            pb.set_message("Contacting server for dates...");
            let response = self.client.get(url).send().await.context("Failed to send request")?;
            let data: Value = response.json().await.context("Failed to parse JSON")?;
    
            let dates = data["data"]["recordFileList"].as_array().unwrap();
            for date in dates {
                let date_str = date["fileName"].as_str().unwrap().to_string();
                let naive_date = NaiveDate::parse_from_str(&date_str, "%Y%m%d").context("Failed to parse date")?;
                // Check if the date is already known and up to the last known date
                if let Some(last_date) = last_known_date {
                    if naive_date <= last_date {
                        continue_fetching = false;
                        break;
                    }
                }
                if naive_date == self.today {
                    continue; // Skip today's files
                }
                records.insert(date_str, DateRecord {
                    downloaded: false,
                    files: Vec::new(),
                });
            }

            // 时间
            if !continue_fetching {
                break;
            }
    
            continue_fetching = data["data"]["isTruncate"].as_bool().unwrap_or(false);
            next_marker = data["data"]["nextMarker"].as_str().map(String::from);
            pb.inc(1);
        }
    
        pb.finish_with_message("Fetching complete.");
        Ok(records)
    }

    async fn fetch_files_for_date(&self, date: &str) -> Result<Vec<FileRecord>> {
        let mut files = Vec::new();
        let mut next_marker: Option<String> = None;
        let mut continue_fetching = true;

        while continue_fetching {
            let mut url = self.base_url.clone();
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            url.query_pairs_mut().append_pair("path", &format!("cdn/okex/traderecords/trades/daily/{}", date));
            url.query_pairs_mut().append_pair("size", "100");
            url.query_pairs_mut().append_pair("t", &timestamp.to_string());
            if let Some(marker) = &next_marker {
                url.query_pairs_mut().append_pair("nextMarker", marker);
            }

            let response = self.client.get(url).send().await.context("Failed to send request")?;
            let data: Value = response.json().await.context("Failed to parse JSON")?;

            let file_list = data["data"]["recordFileList"].as_array().unwrap();
            for file in file_list {
                let file_name = file["fileName"].as_str().unwrap().to_string();
                files.push(FileRecord {
                    file_name,
                });
            }

            continue_fetching = data["data"]["isTruncate"].as_bool().unwrap_or(false);
            next_marker = data["data"]["nextMarker"].as_str().map(String::from);
        }
        Ok(files)
    }

    async fn save_to_file(&self) -> Result<()> {
        let mut data = serde_json::Map::new();

        // 存储最后更新日期
        if let Some(last_update) = self.last_update {
            data.insert("last_update".to_string(), serde_json::Value::String(last_update.format("%Y-%m-%d").to_string()));
        }
    
        // 为每个日期准备一个记录，包含文件列表及其下载状态
        let dates = self.records.iter().map(|(date, date_record)| {
            let files: Vec<serde_json::Value> = date_record.files.iter().map(|file| serde_json::Value::String(file.file_name.clone())).collect();
    
            (
                date.clone(), 
                serde_json::json!({
                    "downloaded": date_record.downloaded,
                    "files": files
                })
            )
        }).collect::<serde_json::Map<_, _>>();
    
        data.insert("dates".to_string(), serde_json::Value::Object(dates));
    
        // 将数据转换为 JSON 格式的字符串
        let json_string = serde_json::to_string_pretty(&data).context("Failed to serialize data to JSON")?;
    
        // 写入文件
        let mut file = File::create(&self.data_store_path).await.context("Failed to create or open the file")?;
        file.write_all(json_string.as_bytes()).await.context("Failed to write data to file")?;
    
        Ok(())
    }

    fn build_file_download_url(&self, date: &str, file_name: &str) -> String {
        let file_download_base_url = format!("https://static.okx.com/cdn/okex/traderecords/{}/daily", self.data_type.as_str());
        format!("{}/{}/{}", file_download_base_url, date, file_name)
    }
    
    pub async fn update_file_lists(&mut self) -> Result<usize> {
        // 创建一个临时集合来存储更新后的文件列表
        let mut updated_records = HashMap::new();
        let pb = ProgressBar::new(self.records.len() as u64);
        log::info!("更新文件列表......");
        pb.set_style(ProgressStyle::default_bar().template("{wide_bar} {pos}/{len} Fetching file lists...")?);

        let mut not_downloaded_count = 0; // 用于计数未下载的记录

        // 迭代复制需要更新的记录
        for (date, record) in self.records.iter() {
            if record.files.is_empty() {
                let files = self.fetch_files_for_date(date).await?;
                updated_records.insert(date.clone(), files);
            }
            if !record.downloaded || self.ignore_undownloaded { // 检查下载状态
                not_downloaded_count += 1;
            }
            pb.inc(1);
        }

        // 将更新后的文件列表回写到原始记录中
        for (date, files) in updated_records {
            if let Some(record) = self.records.get_mut(&date) {
                record.files = files;
            }
        }

        pb.finish_with_message("All file lists fetched and updated.");
        log::info!("文件列表获取成功(共{}天未下载)，保存中....", not_downloaded_count);
        self.save_to_file().await?;  // Save updates after fetching all file lists
        Ok(not_downloaded_count)  // Return the count of updated records
    }

    // 函数来下载还未下载的文件
    pub async fn download_unfetched_files(&mut self, filter: impl Fn(&str) -> bool) -> Result<()> {
        // 先更新一下列表
        let _record_num = self.update_file_lists().await?;

        log::info!("开始下载未下载的文件.....");
        let max_concurrent_downloads = 20;
        let semaphore = Arc::new(Semaphore::new(max_concurrent_downloads));
        let mut tasks: Vec<JoinHandle<Result<(bool, String), anyhow::Error>>> = Vec::new();

        // Separate data for download tasks
        let mut downloads = Vec::new();
        let mut download_status: HashMap<String, bool> = HashMap::new();

        for (date, record) in &self.records {
            let file_path = format!("./data/{}/{}", self.data_type.as_str(), date);
            tokio::fs::create_dir_all(&file_path).await?;
            
            let mut all_exist = true;
            for file in &record.files {
                if filter(&file.file_name) && !Path::new(&format!("{}/{}", file_path, file.file_name)).exists() {
                    let file_url = self.build_file_download_url(date, &file.file_name);
                    let save_path = format!("{}/{}", file_path, file.file_name);
                    downloads.push((file_url, save_path, date.clone()));
                    all_exist = false;
                }
            }
            download_status.insert(date.clone(), all_exist);
        }

        let pb = Arc::new(Mutex::new(ProgressBar::new(downloads.len() as u64)));
        pb.lock().await.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
            .progress_chars("#>-"));

        // Create tasks without reference to self
        for (file_url, save_path, date) in downloads {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let pb_clone = pb.clone();
            tasks.push(tokio::spawn(async move {
                let result = Self::download_and_save_file(file_url, save_path).await;
                drop(permit);
                pb_clone.lock().await.inc(1);
                result.map(|_| (true, date.clone()))
            }));
        }

        log::info!("打包下载任务数量: {}", tasks.len());

        // Process all tasks and handle their results
        for task in tasks {
            let (success, date) = task.await??;
            download_status.insert(date, success);
        }

        pb.lock().await.finish_with_message("All files processed.");

        // Update the records after all downloads
        for (date, status) in download_status {
            if let Some(record) = self.records.get_mut(&date) {
                record.downloaded = status;
            }
        }

        Ok(())
    
    }

    async fn download_and_save_file(file_url: String, save_path: String) -> Result<()> {
        let mut response = reqwest::get(&file_url).await?;
        let mut file = tokio::fs::File::create(save_path).await?;
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await?;
        }
        Ok(())
    }

}