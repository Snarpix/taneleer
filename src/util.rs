use std::{ffi::CString, os::unix::prelude::OsStrExt, path::Path};

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

fn cstr(path: &Path) -> std::io::Result<CString> {
    Ok(CString::new(path.as_os_str().as_bytes())?)
}

#[cfg(target_os = "linux")]
fn rename_no_replace() {}

#[cfg(target_os = "macos")]
pub async fn rename_no_replace(
    from: impl AsRef<Path>,
    to: impl AsRef<Path>,
) -> std::io::Result<()> {
    let from = from.as_ref().to_owned();
    let to = to.as_ref().to_owned();

    match tokio::task::spawn_blocking(move || {
        let old = cstr(&from)?;
        let new = cstr(&to)?;
        match unsafe { libc::renamex_np(old.as_ptr(), new.as_ptr(), libc::RENAME_EXCL) } {
            0 => Ok(()),
            _ => Err(std::io::Error::last_os_error()),
        }
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "background task failed",
        )),
    }
}
