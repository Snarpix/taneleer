use uuid::Uuid;

#[derive(sqlx::FromRow)]
pub struct Tag {
    pub name: String,
    pub value: Option<String>,
}

#[derive(sqlx::FromRow)]
pub struct ArtifactTag {
    pub artifact_uuid: Uuid,
    #[sqlx(flatten)]
    pub tag: Tag,
}
