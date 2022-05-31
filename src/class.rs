pub enum ArtifactType {
    File,
}

impl std::str::FromStr for ArtifactType {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "file" => Ok(ArtifactType::File),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for ArtifactType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactType::File => write!(f, "file"),
        }
    }
}

pub struct ArtifactClassData {
    pub backend_name: String,
    pub art_type: ArtifactType,
}
