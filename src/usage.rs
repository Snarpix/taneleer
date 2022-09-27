use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct ArtifactUsage {
    pub artifact_uuid: Uuid,
    #[sqlx(flatten)]
    pub usage: Usage,
}

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct Usage {
    pub uuid: Uuid,
    pub reserve_time: i64,
}
