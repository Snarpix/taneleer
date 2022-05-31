use std::sync::Arc;

use dbus::channel::{BusType, MatchingReceiver};
use dbus::message::MatchRule;
use dbus::nonblock::SyncConnection;
use dbus::MethodErr;
use dbus_crossroads::{Crossroads, IfaceBuilder};
use dbus_tokio::connection::{self, IOResourceError};
use tokio::task::JoinHandle;

use super::Frontend;
use crate::artifact::Artifact;
use crate::class::{ArtifactClassData, ArtifactType};
use crate::manager::SharedArtifactManager;

pub struct DBusFrontend {
    handle: JoinHandle<IOResourceError>,
    _conn: Arc<SyncConnection>,
}

impl DBusFrontend {
    pub async fn new(
        bus: BusType,
        manager: SharedArtifactManager,
    ) -> Result<DBusFrontend, Box<dyn std::error::Error>> {
        let (resource, conn) = connection::new::<SyncConnection>(bus)?;

        let handle = tokio::spawn(resource);

        let mut cr = Crossroads::new();
        cr.set_async_support(Some((
            conn.clone(),
            Box::new(|x| {
                tokio::spawn(x);
            }),
        )));

        let token = cr.register(
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
                        println!("CreateArtifactClass");
                        async move {
                            let res: Result<(), MethodErr> = async move {
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
                                        "com.snarpix.taneleer.sError.Unknown",
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
        );

        cr.insert(
            "/com/snarpix/taneleer/ArtifactManager",
            &[token],
            manager.clone(),
        );

        let token_class = cr.register(
            "com.snarpix.taneleer.ArtifactClass",
            |b: &mut IfaceBuilder<()>| {
                b.method("Reserve", (), (), move |_, _obj, _: ()| {
                    println!("Reserve");
                    Ok(())
                });
                b.method("Abort", (), (), move |_, _obj, _: ()| {
                    println!("Abort");
                    Ok(())
                });
            },
        );

        cr.insert(
            "/com/snarpix/taneleer/Artifacts/test_class",
            &[token_class],
            (),
        );
        let token_artifact = cr.register(
            "com.snarpix.taneleer.Artifact",
            |b: &mut IfaceBuilder<Artifact>| {
                b.method("Commit", (), (), move |_, _obj, _: ()| {
                    println!("Commit");
                    Ok(())
                });
                b.method("Acquire", (), (), move |_, _obj, _: ()| {
                    println!("Acquire");
                    Ok(())
                });
                b.method("Release", (), (), move |_, _obj, _: ()| {
                    println!("Release");
                    Ok(())
                });
            },
        );

        cr.insert(
            "/com/snarpix/taneleer/Artifacts/test_class/1",
            &[token_artifact],
            Artifact {},
        );

        conn.request_name("com.snarpix.taneleer", false, true, false)
            .await?;

        conn.start_receive(
            MatchRule::new_method_call(),
            Box::new(move |msg, conn| {
                cr.handle_message(msg, conn).unwrap();
                true
            }),
        );
        Ok(DBusFrontend {
            handle,
            _conn: conn,
        })
    }
}

impl Frontend for DBusFrontend {}

impl Drop for DBusFrontend {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
