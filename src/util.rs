use std::path::Path;

use sha2::Digest;
use tokio::io::AsyncReadExt;

use crate::source::Sha256;

pub async fn hash_file_sha256(file_path: &Path) -> Result<Sha256, tokio::io::Error> {
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(file_path)
        .await?;
    let mut buffer = bytes::BytesMut::with_capacity(1 * 1024 * 1024);
    let mut hasher = sha2::Sha256::new();
    while file.read_buf(&mut buffer).await? != 0 {
        let handle = tokio::task::spawn_blocking(move || {
            hasher.update(&mut buffer);
            buffer.clear();
            (hasher, buffer)
        });
        (hasher, buffer) = handle.await?;
    }
    Ok(*hasher.finalize().as_ref())
}
