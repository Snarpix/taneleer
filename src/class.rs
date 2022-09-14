#[derive(Debug, sqlx::Type)]
#[sqlx(rename_all = "snake_case")]
pub enum ArtifactType {
    File,
    DockerContainer,
}

#[derive(Copy, Clone, Debug, sqlx::Type)]
#[repr(i64)]
pub enum ArtifactClassState {
    Uninit = 0,
    Init = 1,
}

// TODO: Use serde for this or give up and use strum
impl std::str::FromStr for ArtifactClassState {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "uninit" => Ok(ArtifactClassState::Uninit),
            "init" => Ok(ArtifactClassState::Init),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for ArtifactClassState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactClassState::Uninit => write!(f, "uninit"),
            ArtifactClassState::Init => write!(f, "init"),
        }
    }
}

impl std::str::FromStr for ArtifactType {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "file" => Ok(ArtifactType::File),
            "docker_container" => Ok(ArtifactType::DockerContainer),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for ArtifactType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactType::File => write!(f, "file"),
            ArtifactType::DockerContainer => write!(f, "docker_container"),
        }
    }
}

#[derive(Debug)]
pub struct ArtifactClassData {
    pub backend_name: String,
    pub art_type: ArtifactType,
}
