use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct ArtifactUsage {
    pub uuid: Uuid,
    pub artifact_uuid: Uuid,
    pub reserve_time: i64,
}
