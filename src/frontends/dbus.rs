use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex as StdMutex};

use dbus::arg::{Dict, RefArg, Variant};
use dbus::channel::{BusType, MatchingReceiver};
use dbus::message::MatchRule;
use dbus::nonblock::SyncConnection;
use dbus::MethodErr;
use dbus_crossroads::{Crossroads, IfaceBuilder, IfaceToken};
use dbus_tokio::connection::{self, IOResourceError};
use futures::StreamExt;
use log::error;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::Frontend;
use crate::artifact::{ArtifactItem, ArtifactItemInfo, ArtifactState};
use crate::class::{ArtifactClassData, ArtifactType};
use crate::error::Result;
use crate::manager::{ArtifactManager, ManagerMessage, SharedArtifactManager};
use crate::source::{Hashsum, Sha1, Sha256, Source, SourceType};
use crate::tag::{ArtifactTag, Tag};
use crate::usage::ArtifactUsage;

pub struct DBusFrontend {
    handle: JoinHandle<IOResourceError>,
    _conn: Arc<SyncConnection>,
    _subscr_handle: JoinHandle<()>,
}

struct DBusFrontendInner {
    cr: Arc<StdMutex<Crossroads>>,
    manager: Arc<TokioMutex<ArtifactManager>>,
    class_iface_token: IfaceToken<ArtifactClass>,
    artifact_reserve_iface_token: IfaceToken<Artifact>,
    artifact_iface_token: IfaceToken<Artifact>,
}

#[derive(Clone)]
struct ArtifactClass {
    name: String,
    manager: SharedArtifactManager,
}

#[derive(Clone)]
struct Artifact {
    uuid: Uuid,
    class_name: String,
    manager: SharedArtifactManager,
}

impl DBusFrontend {
    pub async fn new(
        dbus_name: &str,
        bus: BusType,
        manager: SharedArtifactManager,
    ) -> Result<DBusFrontend> {
        let (resource, conn) = connection::new::<SyncConnection>(bus)?;
        let handle = tokio::spawn(resource);

        let cr = Arc::new(StdMutex::new(Crossroads::new()));
        let subscr_handle = {
            let mut cr_lock = cr.lock().unwrap();

            cr_lock.set_async_support(Some((
                conn.clone(),
                Box::new(|x| {
                    tokio::spawn(x);
                }),
            )));

            let manager_iface_token = Self::register_manager_iface(&mut cr_lock);
            cr_lock.insert(
                "/com/snarpix/taneleer/ArtifactManager",
                &[manager_iface_token],
                manager.clone(),
            );

            let event_stream = {
                let manager = manager.lock().await;
                let new_events = manager.subscribe();
                let old_events =
                    futures::stream::iter(manager.get_init_stream().await.unwrap()).map(Ok);
                old_events.chain(new_events)
            };

            let inner_dbus = Arc::new(DBusFrontendInner::new(cr.clone(), manager, &mut cr_lock));
            let subscription = event_stream.for_each({
                move |m| {
                    let m = m.unwrap();
                    inner_dbus.handle_message(m);
                    async {}
                }
            });

            tokio::spawn(subscription)
        };

        conn.request_name(dbus_name, false, true, false).await?;

        conn.start_receive(
            MatchRule::new_method_call(),
            Box::new(move |msg, conn| {
                let mut cr = cr.lock().unwrap();
                cr.handle_message(msg, conn).unwrap();
                true
            }),
        );
        Ok(DBusFrontend {
            handle,
            _conn: conn,
            _subscr_handle: subscr_handle,
        })
    }

    fn register_manager_iface(cr: &mut Crossroads) -> IfaceToken<Arc<TokioMutex<ArtifactManager>>> {
        cr.register(
            "com.snarpix.taneleer.ArtifactManager",
            |b: &mut IfaceBuilder<SharedArtifactManager>| {
                b.method_with_cr_async(
                    "CreateArtifactClass",
                    ("name", "backend", "type"),
                    (),
                    |mut ctx, cr, (name, backend_name, art_type): (String, String, String)| {
                        let obj = cr
                            .data_mut::<SharedArtifactManager>(ctx.path())
                            .cloned()
                            .ok_or_else(|| MethodErr::no_path(ctx.path()));
                        async move {
                            let res: StdResult<(), MethodErr> = async move {
                                let obj = obj?;
                                let art_type =
                                    art_type.parse::<ArtifactType>().map_err(|_| -> MethodErr {
                                        (
                                            "com.snarpix.taneleer.Error.InvalidArtifactType",
                                            "Invalid artifact type",
                                        )
                                            .into()
                                    })?;
                                if obj
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
                                    return Err((
                                        "com.snarpix.taneleer.Error.Unknown",
                                        "Unknown error",
                                    )
                                        .into());
                                }
                                Ok(())
                            }
                            .await;
                            ctx.reply(res)
                        }
                    },
                );
                b.method_with_cr_async(
                    "GetClasses",
                    (),
                    ("artifact_classes",),
                    move |mut ctx, cr, ()| {
                        let obj = cr
                            .data_mut::<SharedArtifactManager>(ctx.path())
                            .cloned()
                            .ok_or_else(|| MethodErr::no_path(ctx.path()));
                        async move {
                            let res: StdResult<_, MethodErr> = async move {
                                match obj?.lock().await.get_artifact_classes().await {
                                    Ok(r) => Ok(r),
                                    Err(_) => {
                                        Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                            .into())
                                    }
                                }
                            }
                            .await;
                            ctx.reply(res.map(|r| {
                                (Dict::new(
                                    r.into_iter()
                                        .map(|(name, data, state)| {
                                            (
                                                name,
                                                (
                                                    data.backend_name,
                                                    data.art_type.to_string(),
                                                    state.to_string(),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>(),
                                ),)
                            }))
                        }
                    },
                );
                b.method_with_cr_async(
                    "GetArtifacts",
                    (),
                    ("artifacts",),
                    move |mut ctx, cr, ()| {
                        let obj = cr
                            .data_mut::<SharedArtifactManager>(ctx.path())
                            .cloned()
                            .ok_or_else(|| MethodErr::no_path(ctx.path()));
                        async move {
                            let res: StdResult<_, MethodErr> = async move {
                                match obj?.lock().await.get_artifacts_info().await {
                                    Ok(r) => Ok(r),
                                    Err(_) => {
                                        Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                            .into())
                                    }
                                }
                            }
                            .await;
                            ctx.reply(res.map(|r| {
                                (Dict::new(
                                    r.into_iter()
                                        .map(|info| {
                                            (
                                                info.uuid.to_string(),
                                                (
                                                    info.data.class_name,
                                                    info.data.art_type.to_string(),
                                                    info.data.reserve_time,
                                                    info.data.commit_time.unwrap_or(0),
                                                    info.data.use_count,
                                                    <&str>::from(info.data.state).to_owned(),
                                                    info.data
                                                        .next_state
                                                        .map(<&str>::from)
                                                        .unwrap_or("")
                                                        .to_owned(),
                                                    info.data
                                                        .error
                                                        .unwrap_or_else(|| "".to_string()),
                                                ),
                                            )
                                        })
                                        .collect::<Vec<_>>(),
                                ),)
                            }))
                        }
                    },
                );
                b.method_with_cr_async("GetSources", (), ("sources",), move |mut ctx, cr, ()| {
                    let obj = cr
                        .data_mut::<SharedArtifactManager>(ctx.path())
                        .cloned()
                        .ok_or_else(|| MethodErr::no_path(ctx.path()));
                    async move {
                        let res: StdResult<_, MethodErr> = async move {
                            match obj?.lock().await.get_sources().await {
                                Ok(r) => Ok(r),
                                Err(_) => {
                                    Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                        .into())
                                }
                            }
                        }
                        .await;
                        ctx.reply(res.map(|r| {
                            (
                                r.into_iter()
                                    .map(
                                        |(uuid, Source { name, source })| -> (
                                            (String, String),
                                            (&str, Variant<Box<dyn RefArg>>),
                                        ) {
                                            (
                                                (uuid.to_string(), name),
                                                match source {
                                                    SourceType::Artifact { uuid } => (
                                                        "artifact",
                                                        Variant(Box::new(uuid.to_string())),
                                                    ),
                                                    SourceType::Git { repo, commit } => (
                                                        "git",
                                                        Variant(Box::new((
                                                            repo,
                                                            hex::encode(&commit),
                                                        ))),
                                                    ),
                                                    SourceType::Url { url, hash } => {
                                                        let Hashsum::Sha256(s) = hash;
                                                        (
                                                            "url",
                                                            Variant(Box::new((
                                                                url,
                                                                hex::encode(&s),
                                                            ))),
                                                        )
                                                    }
                                                },
                                            )
                                        },
                                    )
                                    .collect::<Vec<_>>(),
                            )
                        }))
                    }
                });
                b.method_with_cr_async("GetItems", (), ("items",), |mut ctx, cr, ()| {
                    let obj = cr
                        .data_mut::<SharedArtifactManager>(ctx.path())
                        .cloned()
                        .ok_or_else(|| MethodErr::no_path(ctx.path()));
                    async move {
                        let res: StdResult<_, MethodErr> = async move {
                            match obj?.lock().await.get_items().await {
                                Ok(r) => Ok(r),
                                Err(_) => {
                                    Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                        .into())
                                }
                            }
                        }
                        .await;
                        ctx.reply(res.map(|r| {
                            (r.into_iter()
                                .map(
                                    |ArtifactItem {
                                         uuid,
                                         info: ArtifactItemInfo { id, size, hash },
                                     }| {
                                        let Hashsum::Sha256(s) = hash;
                                        ((uuid.to_string(), id), (size, hex::encode(&s)))
                                    },
                                )
                                .collect::<Vec<_>>(),)
                        }))
                    }
                });
                b.method_with_cr_async("GetTags", (), ("tags",), |mut ctx, cr, ()| {
                    let obj = cr
                        .data_mut::<SharedArtifactManager>(ctx.path())
                        .cloned()
                        .ok_or_else(|| MethodErr::no_path(ctx.path()));
                    async move {
                        let res: StdResult<_, MethodErr> = async move {
                            match obj?.lock().await.get_tags().await {
                                Ok(r) => Ok(r),
                                Err(_) => {
                                    Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                        .into())
                                }
                            }
                        }
                        .await;
                        ctx.reply(res.map(|r| {
                            (r.into_iter()
                                .map(
                                    |ArtifactTag {
                                         artifact_uuid,
                                         tag: Tag { name, value },
                                     }| {
                                        (
                                            (name, value.unwrap_or_default()),
                                            artifact_uuid.to_string(),
                                        )
                                    },
                                )
                                .collect::<Vec<_>>(),)
                        }))
                    }
                });
                b.method_with_cr_async("GetUsages", (), ("usages",), |mut ctx, cr, ()| {
                    let obj = cr
                        .data_mut::<SharedArtifactManager>(ctx.path())
                        .cloned()
                        .ok_or_else(|| MethodErr::no_path(ctx.path()));
                    async move {
                        let res: StdResult<_, MethodErr> = async move {
                            match obj?.lock().await.get_usages().await {
                                Ok(r) => Ok(r),
                                Err(_) => {
                                    Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                        .into())
                                }
                            }
                        }
                        .await;
                        ctx.reply(res.map(|r| {
                            (r.into_iter()
                                .map(
                                    |ArtifactUsage {
                                         uuid,
                                         artifact_uuid,
                                         reserve_time,
                                     }| {
                                        (
                                            uuid.to_string(),
                                            (artifact_uuid.to_string(), reserve_time),
                                        )
                                    },
                                )
                                .collect::<Vec<_>>(),)
                        }))
                    }
                });

                b.method("FindArtifactByUuid", (), (), move |_, _obj, _: ()| {
                    println!("FindArtifactByUuid");
                    Ok(())
                });
            },
        )
    }
}

impl DBusFrontendInner {
    fn new(
        cr_arc: Arc<StdMutex<Crossroads>>,
        manager: SharedArtifactManager,
        cr: &mut Crossroads,
    ) -> Self {
        let class_iface_token = Self::register_class_iface(cr);
        let artifact_reserve_iface_token = Self::register_artifact_reserve_iface(cr);
        let artifact_iface_token = Self::register_artifact_iface(cr);
        DBusFrontendInner {
            cr: cr_arc,
            manager,
            class_iface_token,
            artifact_reserve_iface_token,
            artifact_iface_token,
        }
    }

    fn handle_message(&self, m: ManagerMessage) {
        match m {
            ManagerMessage::NewClass(class_name) => {
                self.add_artifact_class(class_name);
            }
            ManagerMessage::NewArtifact(class_name, artifact_uuid, artifact_state) => {
                self.add_artifact(class_name, artifact_uuid, artifact_state);
            }
            ManagerMessage::ArtifactUpdate(class_name, artifact_uuid, artifact_state) => {
                self.update_artifact(class_name, artifact_uuid, artifact_state);
            }
            ManagerMessage::RemoveArtifact(class_name, artifact_uuid, artifact_state) => {
                self.remove_artifact(class_name, artifact_uuid, artifact_state);
            }
        };
    }

    fn add_artifact_class(&self, class_name: String) {
        self.cr.lock().unwrap().insert(
            dbus::Path::new(format!("/com/snarpix/taneleer/Classes/{}", class_name)).unwrap(),
            &[self.class_iface_token],
            ArtifactClass {
                name: class_name,
                manager: self.manager.clone(),
            },
        );
    }

    fn add_artifact(&self, class_name: String, artifact_uuid: Uuid, artifact_state: ArtifactState) {
        let mut cr = self.cr.lock().unwrap();
        match artifact_state {
            ArtifactState::Committed => {
                cr.insert(
                    dbus::Path::new(format!(
                        "/com/snarpix/taneleer/Artifacts/{}",
                        artifact_uuid.to_string().replace('-', "_")
                    ))
                    .unwrap(),
                    &[self.artifact_iface_token],
                    Artifact {
                        uuid: artifact_uuid,
                        class_name,
                        manager: self.manager.clone(),
                    },
                );
            }
            ArtifactState::Reserved => {
                cr.insert(
                    dbus::Path::new(format!(
                        "/com/snarpix/taneleer/Artifacts/{}",
                        artifact_uuid.to_string().replace('-', "_")
                    ))
                    .unwrap(),
                    &[self.artifact_reserve_iface_token],
                    Artifact {
                        uuid: artifact_uuid,
                        class_name,
                        manager: self.manager.clone(),
                    },
                );
            }
            _ => {}
        }
    }

    fn update_artifact(
        &self,
        class_name: String,
        artifact_uuid: Uuid,
        artifact_state: ArtifactState,
    ) {
        let mut cr = self.cr.lock().unwrap();
        let path = dbus::Path::new(format!(
            "/com/snarpix/taneleer/Artifacts/{}",
            artifact_uuid.to_string().replace('-', "_")
        ))
        .unwrap();
        match artifact_state {
            ArtifactState::Committed => {
                cr.remove_interface(path.clone(), self.artifact_reserve_iface_token);
                cr.insert(
                    path,
                    &[self.artifact_iface_token],
                    Artifact {
                        uuid: artifact_uuid,
                        class_name,
                        manager: self.manager.clone(),
                    },
                );
            }
            ArtifactState::Reserved => {
                cr.insert(
                    path,
                    &[self.artifact_reserve_iface_token],
                    Artifact {
                        uuid: artifact_uuid,
                        class_name,
                        manager: self.manager.clone(),
                    },
                );
            }
            _ => {}
        }
    }

    fn remove_artifact(
        &self,
        _class_name: String,
        artifact_uuid: Uuid,
        _artifact_state: ArtifactState,
    ) {
        self.cr.lock().unwrap().remove::<Artifact>(
            &dbus::Path::new(format!(
                "/com/snarpix/taneleer/Artifacts/{}",
                artifact_uuid.to_string().replace('-', "_")
            ))
            .unwrap(),
        );
    }

    fn register_class_iface(cr: &mut Crossroads) -> IfaceToken<ArtifactClass> {
        cr.register(
            "com.snarpix.taneleer.ArtifactClass",
            |b: &mut IfaceBuilder<ArtifactClass>| {
                b.method_with_cr_async(
                    "Reserve",
                    ("proxy", "sources", "tags"),
                    ("uuid", "url"),
                    move |mut ctx,
                          cr,
                          (proxy, sources, tags): (
                        String,
                        HashMap<String, (String, Variant<Box<dyn RefArg>>)>,
                        Vec<(String, String)>,
                    )| {
                        let obj = cr
                            .data_mut::<ArtifactClass>(ctx.path())
                            .cloned()
                            .ok_or_else(|| MethodErr::no_path(ctx.path()));
                        async move {
                            let res = async move {
                                let ArtifactClass { name, manager } = obj?;
                                let mut sources_conv = Vec::new();
                                for (source_name, (source_type, source_meta)) in sources {
                                    let meta = match source_type.as_str() {
                                        "url" => {
                                            let mut arg_iter =
                                                source_meta.0.as_iter().ok_or_else(|| {
                                                    MethodErr::invalid_arg(&source_meta)
                                                })?;
                                            let url = arg_iter
                                                .next()
                                                .and_then(|i| i.as_str())
                                                .ok_or_else(|| {
                                                    MethodErr::invalid_arg(&source_meta)
                                                })?;
                                            let hash = arg_iter
                                                .next()
                                                .and_then(|i| i.as_str())
                                                .ok_or_else(|| {
                                                    MethodErr::invalid_arg(&source_meta)
                                                })?;
                                            if arg_iter.next().is_some() {
                                                return Err(MethodErr::invalid_arg(&source_meta));
                                            }
                                            let mut sha256_hash: Sha256 = Default::default();
                                            hex::decode_to_slice(hash, &mut sha256_hash)
                                                .map_err(|_| MethodErr::invalid_arg(&hash))?;
                                            SourceType::Url {
                                                url: url.to_owned(),
                                                hash: Hashsum::Sha256(sha256_hash),
                                            }
                                        }
                                        "git" => {
                                            let mut arg_iter =
                                                source_meta.0.as_iter().ok_or_else(|| {
                                                    MethodErr::invalid_arg(&source_meta)
                                                })?;
                                            let repo = arg_iter
                                                .next()
                                                .and_then(|i| i.as_str())
                                                .ok_or_else(|| {
                                                    MethodErr::invalid_arg(&source_meta)
                                                })?;
                                            let commit = arg_iter
                                                .next()
                                                .and_then(|i| i.as_str())
                                                .ok_or_else(|| {
                                                MethodErr::invalid_arg(&source_meta)
                                            })?;
                                            if arg_iter.next().is_some() {
                                                return Err(MethodErr::invalid_arg(&source_meta));
                                            }
                                            let mut sha1_hash: Sha1 = Default::default();
                                            hex::decode_to_slice(commit, &mut sha1_hash)
                                                .map_err(|_| MethodErr::invalid_arg(&commit))?;
                                            SourceType::Git {
                                                repo: repo.to_owned(),
                                                commit: sha1_hash,
                                            }
                                        }
                                        "artifact" => {
                                            let mut arg_iter =
                                                source_meta.0.as_iter().ok_or_else(|| {
                                                    MethodErr::invalid_arg(&source_meta)
                                                })?;
                                            let uuid = arg_iter
                                                .next()
                                                .and_then(|i| i.as_str())
                                                .ok_or_else(|| {
                                                    MethodErr::invalid_arg(&source_meta)
                                                })?;
                                            if arg_iter.next().is_some() {
                                                return Err(MethodErr::invalid_arg(&source_meta));
                                            }
                                            let uuid = Uuid::parse_str(uuid)
                                                .map_err(|_| MethodErr::invalid_arg(&uuid))?;
                                            SourceType::Artifact { uuid }
                                        }
                                        _ => {
                                            return Err(MethodErr::invalid_arg(&source_type));
                                        }
                                    };
                                    sources_conv.push(Source {
                                        name: source_name,
                                        source: meta,
                                    });
                                }
                                let tags = tags
                                    .into_iter()
                                    .map(|(name, value)| {
                                        let value =
                                            if value.is_empty() { None } else { Some(value) };
                                        Tag { name, value }
                                    })
                                    .collect();

                                let proxy = if proxy.is_empty() { None } else { Some(proxy) };

                                let mut manager = manager.lock().await;
                                match manager
                                    .reserve_artifact(name, sources_conv, tags, proxy)
                                    .await
                                {
                                    Ok((uuid, url)) => Ok((uuid.to_string(), url.to_string())),
                                    Err(e) => {
                                        error!("Failed to reserve artifact: {:?}", e);
                                        Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                            .into())
                                    }
                                }
                            }
                            .await;
                            ctx.reply(res)
                        }
                    },
                );
            },
        )
    }

    fn register_artifact_reserve_iface(cr: &mut Crossroads) -> IfaceToken<Artifact> {
        cr.register(
            "com.snarpix.taneleer.ArtifactReserve",
            |b: &mut IfaceBuilder<Artifact>| {
                b.method_with_cr_async(
                    "Commit",
                    ("tags",),
                    (),
                    move |mut ctx, cr, (tags,): (Vec<(String, String)>,)| {
                        let obj = cr
                            .data_mut::<Artifact>(ctx.path())
                            .cloned()
                            .ok_or_else(|| MethodErr::no_path(ctx.path()));
                        async move {
                            let res = async move {
                                let Artifact {
                                    class_name: _class_name,
                                    uuid,
                                    manager,
                                } = obj?;
                                let tags = tags
                                    .into_iter()
                                    .map(|(name, value)| {
                                        let value =
                                            if value.is_empty() { None } else { Some(value) };
                                        Tag { name, value }
                                    })
                                    .collect();
                                let mut manager = manager.lock().await;
                                match manager.commit_artifact_reserve(uuid, tags).await {
                                    Ok(()) => Ok(()),
                                    Err(e) => {
                                        error!("Failed to commit artifact reserve: {:?}", e);
                                        Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                            .into())
                                    }
                                }
                            }
                            .await;
                            ctx.reply(res)
                        }
                    },
                );
                b.method_with_cr_async("Abort", (), (), move |mut ctx, cr, ()| {
                    let obj = cr
                        .data_mut::<Artifact>(ctx.path())
                        .cloned()
                        .ok_or_else(|| MethodErr::no_path(ctx.path()));
                    async move {
                        let res = async move {
                            let Artifact {
                                class_name: _class_name,
                                uuid,
                                manager,
                            } = obj?;
                            let mut manager = manager.lock().await;
                            match manager.abort_artifact_reserve(uuid).await {
                                Ok(()) => Ok(()),
                                Err(e) => {
                                    error!("Failed to abort artifact reserve: {:?}", e);
                                    Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                        .into())
                                }
                            }
                        }
                        .await;
                        ctx.reply(res)
                    }
                });
            },
        )
    }

    fn register_artifact_iface(cr: &mut Crossroads) -> IfaceToken<Artifact> {
        cr.register(
            "com.snarpix.taneleer.Artifact",
            |b: &mut IfaceBuilder<Artifact>| {
                b.method_with_cr_async(
                    "Get",
                    ("proxy",),
                    ("reserve_uuid", "url"),
                    move |mut ctx, cr, (proxy,): (String,)| {
                        let obj = cr
                            .data_mut::<Artifact>(ctx.path())
                            .cloned()
                            .ok_or_else(|| MethodErr::no_path(ctx.path()));
                        async move {
                            let res = async move {
                                let Artifact {
                                    class_name: _class_name,
                                    uuid,
                                    manager,
                                } = obj?;

                                let proxy = if proxy.is_empty() { None } else { Some(proxy) };

                                let mut manager = manager.lock().await;
                                match manager.get_artifact(uuid, proxy).await {
                                    Ok((reserve_uuid, url)) => {
                                        Ok((reserve_uuid.to_string(), url.to_string()))
                                    }
                                    Err(e) => {
                                        error!("Failed to get artifact: {:?}", e);
                                        Err(("com.snarpix.taneleer.Error.Unknown", "Unknown error")
                                            .into())
                                    }
                                }
                            }
                            .await;
                            ctx.reply(res)
                        }
                    },
                );
            },
        )
    }
}

impl Frontend for DBusFrontend {}

impl Drop for DBusFrontend {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
