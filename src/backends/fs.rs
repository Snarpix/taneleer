use super::Backend;

pub struct FsBackend {
    root_path: std::path::PathBuf,
}

impl FsBackend {
    pub async fn new(root_path: &std::path::Path) -> Result<FsBackend, Box<dyn std::error::Error>> {
        Ok(FsBackend {
            root_path: root_path.to_owned(),
        })
    }
}

impl Backend for FsBackend {}
