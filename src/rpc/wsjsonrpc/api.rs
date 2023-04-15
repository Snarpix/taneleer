use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    artifact::{ArtifactData, ArtifactItemInfo},
    class::ArtifactType,
    source::Source,
    tag::Tag,
    usage::Usage,
};

#[derive(Serialize, Deserialize)]
pub struct CreateArtifactClass {
    pub name: String,
    pub backend_name: String,
    pub art_type: ArtifactType,
}

#[derive(Serialize, Deserialize)]
pub struct GetArtifact {
    pub uuid: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct GetArtifactRes {
    pub info: ArtifactData,
    pub sources: Vec<Source>,
    pub tags: Vec<Tag>,
    pub items: Vec<ArtifactItemInfo>,
    pub usages: Vec<Usage>,
}

#[derive(Serialize, Deserialize)]
pub struct ReserveArtifact {
    pub class_name: String,
    pub sources: Vec<Source>,
    pub tags: Vec<Tag>,
}

#[derive(Serialize, Deserialize)]
pub struct ReserveArtifactRes {
    pub uuid: Uuid,
    pub url: String,
}

#[derive(Serialize, Deserialize)]
pub struct CommitArtifact {
    pub uuid: Uuid,
    pub tags: Vec<Tag>,
}

#[derive(Serialize, Deserialize)]
pub struct AbortReserve {
    pub uuid: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct UseArtifact {
    pub uuid: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct UseArtifactRes {
    pub uuid: Uuid,
    pub url: String,
}

#[derive(Serialize, Deserialize)]
pub struct FindLastArtifact {
    pub class_name: String,
    pub sources: Vec<Source>,
    pub tags: Vec<Tag>,
}

#[derive(Serialize, Deserialize)]
pub struct FindLastArtifactRes {
    pub uuid: Uuid,
}

#[derive(Serialize, Deserialize)]
pub struct UseLastArtifact {
    pub class_name: String,
    pub sources: Vec<Source>,
    pub tags: Vec<Tag>,
}

#[derive(Serialize, Deserialize)]
pub struct UseLastArtifactRes {
    pub usage_uuid: Uuid,
    pub artifact_uuid: Uuid,
    pub url: String,
}
