use sqlx::{sqlite::SqliteRow, Row};

use crate::source::Hashsum;

#[derive(Copy, Clone, Debug, sqlx::Type)]
#[repr(i64)]
pub enum ArtifactState {
    Created = 0,
    Reserved = 1,
    Committed = 2,
    Deleted = 3,
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
