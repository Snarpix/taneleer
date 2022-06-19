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
