// main.rs
mod data_fetcher;
use data_fetcher::{DataType, DateFilesFetcher};

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Sets the data type to process
    #[arg(short, value_enum)]
    data_type: DataType,

    /// Sets if undownloaded files should be ignored
    #[arg(short, long)]
    set_ignore_undownloaded: bool,

    /// Positive filter string for filenames
    #[arg(short, long)]
    include_filter: Option<Vec<String>>,

    /// Negative filter string for filenames
    #[arg(short, long)]
    exclude_filter: Option<Vec<String>>,
}

fn setup_logging() -> anyhow::Result<()> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.level(),
                message
            ))
        })
        // 设置日志的最低级别为 Debug 以捕获所有消息
        .level(log::LevelFilter::Info)
        // 将日志输出到标准输出
        .chain(std::io::stdout())
        // 同时将日志输出到文件
        .chain(fern::log_file("pull.log")?)
        // 应用配置
        .apply()?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging().expect("配置日志系统失败");

    let args = Args::parse();

    let mut fetcher = DateFilesFetcher::new(
        args.data_type
    ).await?;

    if args.set_ignore_undownloaded {
        log::info!("进入文件修复模式....");
        fetcher.set_ignore_undownloaded();
    }

    fetcher.download_unfetched_files(|file_name| {
        let mut include = true;  // Assume it's included unless specified
        if let Some(ref includes) = args.include_filter {
            include = includes.iter().any(|inc| file_name.contains(inc));
        }

        let mut exclude = false;  // Assume it's not excluded unless specified
        if let Some(ref excludes) = args.exclude_filter {
            exclude = excludes.iter().any(|exc| file_name.contains(exc));
        }

        include && !exclude
    }).await?;

    Ok(())
}
