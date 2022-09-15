use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub struct ArtifactUsage {
    pub uuid: Uuid,
    pub artifact_uuid: Uuid,
    pub reserve_time: i64,
}
