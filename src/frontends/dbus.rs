use std::result::Result as StdResult;
use std::sync::{Arc, Mutex as StdMutex};

use dbus::arg::Append;
use dbus::channel::{BusType, MatchingReceiver};
use dbus::message::MatchRule;
use dbus::nonblock::SyncConnection;
use dbus::MethodErr;
use dbus_crossroads::{Crossroads, IfaceBuilder, IfaceToken};
use dbus_tokio::connection::{self, IOResourceError};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::Frontend;
use crate::artifact::Artifact;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::error::Result;
use crate::manager::{ArtifactManager, ManagerMessage, SharedArtifactManager};

pub struct DBusFrontend {
    handle: JoinHandle<IOResourceError>,
    _conn: Arc<SyncConnection>,
}

struct ArtifactClass {
    name: String,
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
        let manager_clone = manager.clone();
        let class_iface_token;
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

            let subscription = manager.lock().await.subscribe().for_each({
                let cr = cr_clone.clone();
                let manager = manager.clone();
                let class_iface_token = class_iface_token.clone();
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
                            async {}
                        }
                    }
                }
            });

            tokio::spawn(subscription);
        }

        let artifact_classes_names = manager.lock().await.get_clases().await.unwrap();
        {
            let mut cr_lock = cr.lock().unwrap();
            for c in artifact_classes_names.into_iter() {
                Self::add_artifact_class(&mut cr_lock, manager.clone(), &class_iface_token, c);
            }
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
                b.method("Reserve", (), (), move |_, _obj, _: ()| {
                    println!("Reserve");
                    Ok(())
                });
                b.method("Abort", (), (), move |_, _obj, _: ()| {
                    println!("Abort");
                    Ok(())
                });
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
                manager: manager,
            },
        );
    }
}

impl Frontend for DBusFrontend {}

impl Drop for DBusFrontend {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
