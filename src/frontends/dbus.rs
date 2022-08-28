use std::collections::HashMap;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex as StdMutex};

use dbus::arg::{RefArg, Variant};
use dbus::channel::{BusType, MatchingReceiver};
use dbus::message::MatchRule;
use dbus::nonblock::SyncConnection;
use dbus::MethodErr;
use dbus_crossroads::{Crossroads, IfaceBuilder, IfaceToken};
use dbus_tokio::connection::{self, IOResourceError};
use futures::StreamExt;
use log::error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::Frontend;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::error::Result;
use crate::manager::{ArtifactManager, ManagerMessage, SharedArtifactManager};
use crate::source::{Hashsum, Sha1, Sha256, Source};

pub struct DBusFrontend {
    handle: JoinHandle<IOResourceError>,
    _conn: Arc<SyncConnection>,
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
        let cr_clone = cr.clone();
        let class_iface_token;
        let artifact_reserve_iface_token;
        let artifact_iface_token;
        {
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

            class_iface_token = Self::register_class_iface(&mut cr_lock);
            artifact_reserve_iface_token = Self::register_artifact_reserve_iface(&mut cr_lock);
            artifact_iface_token = Self::register_artifact_iface(&mut cr_lock);
            let event_stream = {
                let manager = manager.lock().await;
                let new_events = manager.subscribe();
                let old_events =
                    futures::stream::iter(manager.get_init_stream().await.unwrap()).map(Ok);
                old_events.chain(new_events)
            };
            let subscription = event_stream.for_each({
                let cr = cr_clone.clone();
                let manager = manager.clone();
                move |m| {
                    let m = m.unwrap();
                    match m {
                        ManagerMessage::NewClass(class_name) => {
                            let mut cr_lock = cr.lock().unwrap();
                            Self::add_artifact_class(
                                &mut cr_lock,
                                manager.clone(),
                                &class_iface_token,
                                class_name,
                            );
                        }
                        ManagerMessage::NewArtifactReserve(class_name, artifact_uuid) => {
                            let mut cr_lock = cr.lock().unwrap();
                            Self::add_artifact_reserve(
                                &mut cr_lock,
                                manager.clone(),
                                &artifact_reserve_iface_token,
                                class_name,
                                artifact_uuid,
                            );
                        }
                        ManagerMessage::RemoveArtifactReserve(class_name, artifact_uuid) => {
                            let mut cr_lock = cr.lock().unwrap();
                            Self::remove_artifact_reserve(
                                &mut cr_lock,
                                manager.clone(),
                                class_name,
                                artifact_uuid,
                            );
                        }
                        ManagerMessage::NewArtifact(class_name, artifact_uuid) => {
                            let mut cr_lock = cr.lock().unwrap();
                            Self::add_artifact_reserve(
                                &mut cr_lock,
                                manager.clone(),
                                &artifact_iface_token,
                                class_name,
                                artifact_uuid,
                            );
                        }
                    };
                    async {}
                }
            });

            tokio::spawn(subscription);
        }

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
        })
    }

    fn register_manager_iface(cr: &mut Crossroads) -> IfaceToken<Arc<Mutex<ArtifactManager>>> {
        cr.register(
            "com.snarpix.taneleer.ArtifactManager",
            |b: &mut IfaceBuilder<SharedArtifactManager>| {
                b.method_with_cr_async(
                    "CreateArtifactClass",
                    ("name", "backend", "type"),
                    (),
                    move |mut ctx, cr, (name, backend_name, art_type): (String, String, String)| {
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
                b.method("FindArtifactByUuid", (), (), move |_, _obj, _: ()| {
                    println!("FindArtifactByUuid");
                    Ok(())
                });
            },
        )
    }

    fn register_class_iface(cr: &mut Crossroads) -> IfaceToken<ArtifactClass> {
        cr.register(
            "com.snarpix.taneleer.ArtifactClass",
            |b: &mut IfaceBuilder<ArtifactClass>| {
                b.method_with_cr_async(
                    "Reserve",
                    ("sources", "tags"),
                    ("uuid", "url"),
                    move |mut ctx,
                          cr,
                          (sources, tags): (
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
                                            Source::Url {
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
                                            Source::Git {
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
                                            Source::Artifact { uuid }
                                        }
                                        _ => {
                                            return Err(MethodErr::invalid_arg(&source_type));
                                        }
                                    };
                                    sources_conv.push((source_name, meta));
                                }
                                let tags = tags
                                    .into_iter()
                                    .map(|(name, value)| {
                                        let value =
                                            if value.is_empty() { None } else { Some(value) };
                                        (name, value)
                                    })
                                    .collect();

                                let mut manager = manager.lock().await;
                                match manager.reserve_artifact(name, sources_conv, tags).await {
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

    fn add_artifact_class(
        cr: &mut Crossroads,
        manager: SharedArtifactManager,
        token: &IfaceToken<ArtifactClass>,
        class_name: String,
    ) {
        cr.insert(
            dbus::Path::new(format!("/com/snarpix/taneleer/Artifacts/{}", class_name)).unwrap(),
            &[*token],
            ArtifactClass {
                name: class_name,
                manager,
            },
        );
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
                                        (name, value)
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
                    (),
                    ("reserve_uuid", "url"),
                    move |mut ctx, cr, ()| {
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
                                match manager.get_artifact(uuid).await {
                                    Ok((reserve_uuid, url)) => Ok((reserve_uuid.to_string(), url)),
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
                    },
                );
            },
        )
    }

    fn add_artifact_reserve(
        cr: &mut Crossroads,
        manager: SharedArtifactManager,
        token: &IfaceToken<Artifact>,
        class_name: String,
        artifact_uuid: Uuid,
    ) {
        cr.insert(
            dbus::Path::new(format!(
                "/com/snarpix/taneleer/Artifacts/{}/{}",
                class_name,
                artifact_uuid.to_string().replace('-', "_")
            ))
            .unwrap(),
            &[*token],
            Artifact {
                uuid: artifact_uuid,
                class_name,
                manager,
            },
        );
    }

    fn remove_artifact_reserve(
        cr: &mut Crossroads,
        manager: SharedArtifactManager,
        class_name: String,
        artifact_uuid: Uuid,
    ) {
        cr.remove::<Artifact>(
            &dbus::Path::new(format!(
                "/com/snarpix/taneleer/Artifacts/{}/{}",
                class_name,
                artifact_uuid.to_string().replace('-', "_")
            ))
            .unwrap(),
        );
    }
}

impl Frontend for DBusFrontend {}

impl Drop for DBusFrontend {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
