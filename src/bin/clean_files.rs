use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use std::path::{PathBuf, Path};
use anyhow::Result;
use std::ffi::OsStr;
use indicatif::{ProgressBar, ProgressStyle};

#[tokio::main]
async fn main() -> Result<()> {
    let path = PathBuf::from("data");
    process_directory(&path).await
}

async fn process_directory(path: &Path) -> Result<()> {
    let mut dirs = vec![path.to_path_buf()];
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.green} [{elapsed_precise}] {wide_msg}")?);

    while let Some(dir) = dirs.pop() {
        let mut entries = fs::read_dir(&dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension() == Some(OsStr::new("zip")) {
                pb.set_message(format!("Processing ZIP: {:?}", path.display()));
                process_zip_file(&path, &pb).await?;
            }
        }
    }
    pb.finish_with_message("Processing complete");
    Ok(())
}

async fn process_zip_file(path: &PathBuf, pb: &ProgressBar) -> Result<()> {
    let metadata = fs::metadata(path).await?;
    if metadata.len() < 100 {
        let mut contents = String::new();
        let mut file = File::open(path).await?;
        file.read_to_string(&mut contents).await?;

        if contents.contains(r#"<Code>NoSuchKey</Code>"#) {
            pb.set_message(format!("Deleting invalid ZIP: {:?}", path.display()));
            fs::remove_file(path).await?;
            pb.set_message("File deleted successfully");
        }
    }

    Ok(())
}
