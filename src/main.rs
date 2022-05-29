use std::sync::{Arc, Mutex};

use dbus::channel::MatchingReceiver;
use dbus::message::MatchRule;
use dbus_crossroads::{Crossroads, IfaceBuilder};
use dbus_tokio::connection;

struct ArtifactManager {}

struct ArtifactsClass {}

struct Artifact {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (resource, c) = connection::new_session_sync()?;

    let handle = tokio::spawn(async {
        let err = resource.await;
        panic!("Lost connection to D-Bus: {}", err);
    });

    let cr = Arc::new(Mutex::new(Crossroads::new()));

    {
        let mut cr_lock = cr.lock().unwrap();

        let token = cr_lock.register(
            "com.snarpix.taneleer.ArtifactManager",
            |b: &mut IfaceBuilder<ArtifactManager>| {
                b.method("FindArtifactByUuid", (), (), move |_, _obj, _: ()| {
                    println!("FindArtifactByUuid");
                    Ok(())
                });
            },
        );

        cr_lock.insert(
            "/com/snarpix/taneleer/ArtifactManager",
            &[token],
            ArtifactManager {},
        );

        let token_class = cr_lock.register(
            "com.snarpix.taneleer.ArtifactsClass",
            |b: &mut IfaceBuilder<ArtifactsClass>| {
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
            ArtifactsClass {},
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

    c.request_name("com.snarpix.taneleer", false, true, false)
        .await?;

    c.start_receive(
        MatchRule::new_method_call(),
        Box::new(move |msg, conn| {
            let mut cr_lock = cr.lock().unwrap();
            cr_lock.handle_message(msg, conn).unwrap();
            true
        }),
    );

    tokio::signal::ctrl_c().await?;

    handle.abort();
    drop(c);

    Ok(())
}
