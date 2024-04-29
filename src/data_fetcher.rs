// data_fetcher.rs
use anyhow::{Result, Context};
use chrono::{Local, NaiveDate};
use clap::ValueEnum;
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, Url};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use toml::Value as TomlValue;

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
}

impl DateFilesFetcher {
    pub async fn new(data_type: DataType) -> Result<Self> {
        let client = Client::new();
        let base_url = Url::parse("https://www.okx.com/priapi/v5/broker/public/v2/orderRecord").unwrap();
        let mut fetcher = DateFilesFetcher {
            client,
            base_url,
            data_store_path: format!("{}.toml", data_type.as_str()),
            records: HashMap::new(),
            last_update: None,
            ignore_undownloaded: false,
            data_type,
        };

        // 尝试加载本地文件
        if Path::new(&fetcher.data_store_path).exists() {
            // 尝试加载，如果成功则不动，失败则拉取数据
            if let Err(_) = fetcher.load_from_file().await {
                // 如果加载失败，则从网络重新拉取
                fetcher.fetch_and_save().await?;
            } else {
                // 检查数据是否是最新的，如果不是今天的，则更新
                if Some(Local::today().naive_local()) != fetcher.last_update {
                    fetcher.fetch_and_save().await?;
                }
            }
        } else {
            // 文件不存在，直接拉取数据
            fetcher.fetch_and_save().await?;
        }
        Ok(fetcher)
    }

    pub fn set_ignore_undownloaded(&mut self) {
        self.ignore_undownloaded = true;
    }

    async fn load_from_file(&mut self) -> Result<()> {
        let mut file = File::open(&self.data_store_path).await.context("Failed to open file")?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).await.context("Failed to read file")?;
        let toml_data: TomlValue = toml::from_str(&contents).context("Failed to parse TOML data")?;

        self.records = toml_data["dates"].as_table().unwrap().iter().map(|(date, record)| {
            let record = record.as_table().unwrap();
            let files = record["files"].as_array().unwrap().iter().map(|file| {
                let file = file.as_table().unwrap();
                FileRecord {
                    file_name: file["file_name"].as_str().unwrap().to_string(),
                }
            }).collect();

            (date.clone(), DateRecord {
                downloaded: record["downloaded"].as_bool().unwrap(),
                files,
            })
        }).collect();

        self.last_update = Some(NaiveDate::parse_from_str(toml_data["last_update"].as_str().unwrap(), "%Y-%m-%d")?);
        Ok(())
    }

    async fn fetch_and_save(&mut self) -> Result<()> {
        // Assume fetch_dates now takes an optional NaiveDate and fetches dates after this date
        self.records = self.fetch_dates(self.last_update).await?;
        self.last_update = Some(Local::today().naive_local()); // Update the last update time to today
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
                records.insert(date_str, DateRecord {
                    downloaded: false,
                    files: Vec::new(),
                });
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
        let mut data = toml::value::Table::new();

        // 存储最后更新日期
        if let Some(last_update) = self.last_update {
            data.insert(
                "last_update".to_string(), 
                toml::Value::String(last_update.format("%Y-%m-%d").to_string())
            );
        }

        // 为每个日期准备一个子表，包含文件列表及其下载状态
        let dates_table = self.records.iter().map(|(date, date_record)| {
            let files_array = toml::Value::Array(date_record.files.iter().map(|file| {
                let mut file_table = toml::value::Table::new();
                file_table.insert("file_name".to_string(), toml::Value::String(file.file_name.clone()));
                toml::Value::Table(file_table)
            }).collect());

            let mut date_table = toml::value::Table::new();
            date_table.insert("downloaded".to_string(), toml::Value::Boolean(date_record.downloaded));
            date_table.insert("files".to_string(), files_array);

            (date.clone(), toml::Value::Table(date_table))
        }).collect();

        data.insert("dates".to_string(), toml::Value::Table(dates_table));

        // 将数据转换为 TOML 格式的字符串
        let toml_string = toml::to_string(&data).context("Failed to serialize data to TOML")?;

        // 写入文件
        let mut file = File::create(&self.data_store_path).await.context("Failed to create or open the file")?;
        file.write_all(toml_string.as_bytes()).await.context("Failed to write data to file")?;

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
        self.save_to_file().await?;  // Save updates after fetching all file lists
        Ok(not_downloaded_count)  // Return the count of updated records
    }

    // 函数来下载还未下载的文件
    pub async fn download_unfetched_files(&mut self, filter: impl Fn(&str) -> bool) -> Result<()> {
        let updated_count = self.update_file_lists().await?;

        let pb = ProgressBar::new(updated_count as u64);
        pb.set_style(ProgressStyle::default_bar().template("{wide_bar} {pos}/{len} {msg}")?);
    
        // 创建一个新的HashMap以更新记录的下载状态
        let mut download_status = HashMap::new();

        for (date, record) in &self.records {
            let need_download = !record.downloaded || self.ignore_undownloaded;
            if need_download && !record.files.is_empty() {
                println!("Fetching files for date: {}", date);
                pb.set_message(format!("Processing {}", date));
                let file_path = format!("./data/{}/{}", self.data_type.as_str(), date);

                // 确保目标目录存在
                fs::create_dir_all(&file_path).await.context("Failed to create directory for file downloads")?;

                let download_tasks: Vec<_> = record.files.iter().filter(|file| filter(&file.file_name))
                    .map(|file| {
                        let file_url = self.build_file_download_url(date, &file.file_name);
                        let save_path = format!("{}/{}", file_path, file.file_name);
                        self.download_and_save_file(file_url, save_path)
                    }).collect();

                // 使用 join_all 等待所有下载任务完成
                let mut all_successful = true;
                let results = join_all(download_tasks).await;
                for result in results {
                    match result {
                        Ok(filename) => println!("Download completed. -- {filename}"),
                        Err(e) => {
                            eprintln!("Download failed: {}", e);
                            all_successful = false;
                        },
                    }
                }

                if all_successful {
                    download_status.insert(date.clone(), true);
                }
            }
            pb.inc(1);
        }

        // 更新下载状态
        for (date, status) in download_status {
            if let Some(record) = self.records.get_mut(&date) {
                record.downloaded = status;
            }
        }

        // 保存更新后的记录
        self.save_to_file().await?;
        pb.finish_with_message("All files processed.");
        Ok(())
    }

    async fn download_and_save_file(&self, file_url: String, save_path: String) -> Result<String> {
        if fs::metadata(&save_path).await.is_ok() {
            return Ok(save_path.to_owned());
        }
        let response = self.client.get(&file_url).send().await
            .with_context(|| format!("Failed to read bytes from response for URL {}", file_url))?;
        let content = response
            .bytes()
            .await
            .with_context(|| format!("Failed to create file at {}", save_path))?;
        let mut file = File::create(&save_path).await?;
        file.write_all(&content)
            .await
            .with_context(|| format!("Failed to write data to file at {}", save_path))?;
        Ok(save_path.to_owned())
    }

}