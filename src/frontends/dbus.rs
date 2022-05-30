use std::sync::{Arc, Mutex};

use dbus::channel::{BusType, MatchingReceiver};
use dbus::message::MatchRule;
use dbus::nonblock::SyncConnection;
use dbus_crossroads::{Crossroads, IfaceBuilder};
use dbus_tokio::connection::{self, IOResourceError};
use tokio::task::JoinHandle;

use super::Frontend;
use crate::artifact::Artifact;
use crate::class::ArtifactClass;
use crate::manager::SharedArtifactManager;

pub struct DBusFrontend {
    handle: JoinHandle<IOResourceError>,
    conn: Arc<SyncConnection>,
}

impl DBusFrontend {
    pub async fn new(
        bus: BusType,
        manager: SharedArtifactManager,
    ) -> Result<DBusFrontend, Box<dyn std::error::Error>> {
        let (resource, conn) = connection::new::<SyncConnection>(bus)?;

        let handle = tokio::spawn(resource);

        let cr = Arc::new(Mutex::new(Crossroads::new()));

        {
            let mut cr_lock = cr.lock().unwrap();

            let token = cr_lock.register(
                "com.snarpix.taneleer.ArtifactManager",
                |b: &mut IfaceBuilder<SharedArtifactManager>| {
                    b.method("FindArtifactByUuid", (), (), move |_, _obj, _: ()| {
                        println!("FindArtifactByUuid");
                        Ok(())
                    });
                },
            );

            cr_lock.insert(
                "/com/snarpix/taneleer/ArtifactManager",
                &[token],
                manager.clone(),
            );

            let token_class = cr_lock.register(
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
            );

            cr_lock.insert(
                "/com/snarpix/taneleer/Artifacts/test_class",
                &[token_class],
                ArtifactClass {},
            );
            let token_artifact = cr_lock.register(
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

            cr_lock.insert(
                "/com/snarpix/taneleer/Artifacts/test_class/1",
                &[token_artifact],
                Artifact {},
            )
        }

        conn.request_name("com.snarpix.taneleer", false, true, false)
            .await?;

        conn.start_receive(
            MatchRule::new_method_call(),
            Box::new(move |msg, conn| {
                let mut cr_lock = cr.lock().unwrap();
                cr_lock.handle_message(msg, conn).unwrap();
                true
            }),
        );
        Ok(DBusFrontend { handle, conn })
    }

    pub fn close(self) {
        self.handle.abort();
    }
}

impl Frontend for DBusFrontend {}
