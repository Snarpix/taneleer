use crate::{class::ArtifactClassData, error::Error, manager::SharedArtifactManager};

use super::api::*;
use super::jsonrpc::*;

pub async fn create_artifact_class(
    CreateArtifactClass {
        name,
        backend_name,
        art_type,
    }: CreateArtifactClass,
    manager: SharedArtifactManager,
) -> RpcResult {
    if manager
        .lock()
        .await
        .create_class(
            name,
            ArtifactClassData {
                backend_name,
                art_type,
            },
        )
        .await
        .is_err()
    {
        RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        }
    } else {
        RpcResult::Result(serde_json::Value::Null)
    }
}

pub async fn get_classes((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_artifact_classes().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn get_artifacts((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_artifacts_info().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn get_sources((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_sources().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn get_items((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_items().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn get_tags((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_tags().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn get_usages((): (), manager: SharedArtifactManager) -> RpcResult {
    match manager.lock().await.get_usages().await {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn get_artifact(
    GetArtifact { uuid }: GetArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    let manager = manager.lock().await;
    let res: Result<_, Error> = async {
        let info = manager.get_artifact_info(uuid).await?;
        let sources = manager.get_artifact_sources(uuid).await?;
        let tags = manager.get_artifact_tags(uuid).await?;
        let items = manager.get_artifact_items(uuid).await?;
        let usages = manager.get_artifact_usages(uuid).await?;
        Ok(GetArtifactRes {
            info,
            sources,
            tags,
            items,
            usages,
        })
    }
    .await;
    match res {
        Ok(r) => RpcResult::Result(serde_json::to_value(r).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn reserve_artifact(
    ReserveArtifact {
        class_name,
        sources,
        tags,
    }: ReserveArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager
        .lock()
        .await
        .reserve_artifact(class_name, sources, tags)
        .await
    {
        Ok((uuid, url)) => RpcResult::Result(
            serde_json::to_value(ReserveArtifactRes {
                uuid,
                url: url.to_string(),
            })
            .unwrap(),
        ),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn commit_artifact(
    CommitArtifact { uuid, tags }: CommitArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager
        .lock()
        .await
        .commit_artifact_reserve(uuid, tags)
        .await
    {
        Ok(()) => RpcResult::Result(serde_json::Value::Null),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn abort_reserve(
    AbortReserve { uuid }: AbortReserve,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager.lock().await.abort_artifact_reserve(uuid).await {
        Ok(()) => RpcResult::Result(serde_json::Value::Null),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn use_artifact(
    UseArtifact { uuid }: UseArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager.lock().await.use_artifact(uuid).await {
        Ok((uuid, url)) => RpcResult::Result(
            serde_json::to_value(UseArtifactRes {
                uuid,
                url: url.to_string(),
            })
            .unwrap(),
        ),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn find_last_artifact(
    FindLastArtifact {
        class_name,
        sources,
        tags,
    }: FindLastArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager
        .lock()
        .await
        .find_last_artifact(class_name, sources, tags)
        .await
    {
        Ok(uuid) => RpcResult::Result(serde_json::to_value(FindLastArtifactRes { uuid }).unwrap()),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}

pub async fn use_last_artifact(
    UseLastArtifact {
        class_name,
        sources,
        tags,
    }: UseLastArtifact,
    manager: SharedArtifactManager,
) -> RpcResult {
    match manager
        .lock()
        .await
        .use_last_artifact(class_name, sources, tags)
        .await
    {
        Ok((usage_uuid, artifact_uuid, url)) => RpcResult::Result(
            serde_json::to_value(UseLastArtifactRes {
                usage_uuid,
                artifact_uuid,
                url: url.to_string(),
            })
            .unwrap(),
        ),
        Err(_) => RpcResult::Error {
            code: ErrorCode::UnknownError as i64,
            message: Some("Unknown error".to_owned()),
            data: None,
        },
    }
}
