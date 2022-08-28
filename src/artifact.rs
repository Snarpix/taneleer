use sqlx::Row;

use crate::source::Hashsum;

#[derive(Copy, Clone)]
pub enum ArtifactState {
    CommitFailed = -1,
    StartReserve = 0,
    Reserve = 1,
    StartCommit = 2,
    Commit = 3,
    StartRemove = 4,
}

pub struct ArtifactItemInfo {
    pub id: String,
    pub size: u64,
    pub hash: Hashsum,
}

impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for ArtifactItemInfo {
    fn from_row(row: &sqlx::sqlite::SqliteRow) -> sqlx::Result<Self> {
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
