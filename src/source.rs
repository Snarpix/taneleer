use uuid::Uuid;

pub type Sha1 = [u8; 20];
pub type Sha256 = [u8; 32];

pub enum Hashsum {
    Sha256(Sha256),
}

pub enum Source {
    Url { url: String, hash: Hashsum },
    Git { repo: String, commit: Sha1 },
    Artifact { uuid: Uuid },
}
