#[derive(sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum ArtifactType {
    File,
}

#[derive(Copy, Clone, Debug, sqlx::Type)]
#[repr(i64)]
pub enum ArtifactClassState {
    Uninit = 0,
    Init = 1,
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
