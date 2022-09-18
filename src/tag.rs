use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct Tag {
    pub name: String,
    pub value: Option<String>,
}

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct ArtifactTag {
    pub artifact_uuid: Uuid,
    #[sqlx(flatten)]
    pub tag: Tag,
}
