use sqlx::{sqlite::SqliteRow, Row};
use uuid::Uuid;

use crate::{class::ArtifactType, source::Hashsum};

#[derive(Copy, Clone, Debug, sqlx::Type, strum::EnumString, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
#[repr(i64)]
pub enum ArtifactState {
    Created = 0,
    Reserved = 1,
    Committed = 2,
    Deleted = 3,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ArtifactData {
    pub class_name: String,
    pub art_type: ArtifactType,
    pub reserve_time: i64,
    pub commit_time: Option<i64>,
    pub use_count: i64,
    pub state: ArtifactState,
    pub next_state: Option<ArtifactState>,
    pub error: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Artifact {
    pub uuid: Uuid,
    #[sqlx(flatten)]
    pub data: ArtifactData,
}

pub struct ArtifactItemInfo {
    pub id: String,
    pub size: u64,
    pub hash: Hashsum,
}

impl sqlx::FromRow<'_, SqliteRow> for ArtifactItemInfo {
    fn from_row(row: &SqliteRow) -> sqlx::Result<Self> {
        let hash_type = row.try_get("hash_type")?;
        let hash_cnt = row.try_get("hash")?;
        let hash =
            Hashsum::from_split(hash_type, hash_cnt).map_err(|e| sqlx::Error::Decode(e.into()))?;
        Ok(Self {
            id: row.try_get("id")?,
            size: row.try_get::<i64, _>("size")? as u64,
            hash,
        })
    }
}
