use std::{collections::HashMap, time::Duration};

use clap::{Parser, Subcommand};
use comfy_table::*;
use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use dbus::arg::{RefArg, Variant};
use dbus::blocking::{Connection, Proxy};

fn get_artifact_manager_proxy(conn: &Connection) -> Proxy<&Connection> {
    conn.with_proxy(
        "com.snarpix.taneleer",
        "/com/snarpix/taneleer/ArtifactManager",
        Duration::from_millis(5000),
    )
}

fn create_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_width(120);
    table
}

#[derive(Parser)]
#[clap(author, version)]
struct Cli {
    #[clap(subcommand)]
    command: MainCommands,
}

#[derive(Subcommand)]
enum MainCommands {
    Class {
        #[clap(subcommand)]
        cmd: ClassCommands,
    },
    Artifact {
        #[clap(subcommand)]
        cmd: ArtifactCommands,
    },
    Source {
        #[clap(subcommand)]
        cmd: SourceCommands,
    },
}

#[derive(Subcommand)]
enum ClassCommands {
    List,
}

fn do_class_cmd(conn: &Connection, cmd: &ClassCommands) {
    match cmd {
        ClassCommands::List => {
            let (classes,): (HashMap<String, (String, String, String)>,) =
                get_artifact_manager_proxy(conn)
                    .method_call("com.snarpix.taneleer.ArtifactManager", "GetClasses", ())
                    .unwrap();
            let mut table = create_table();
            table.set_header(vec![
                Cell::new("Class name").add_attribute(Attribute::Bold),
                Cell::new("Backend name"),
                Cell::new("Artifact type"),
                Cell::new("State"),
            ]);

            for c in classes {
                table.add_row(vec![
                    Cell::new(c.0),
                    Cell::new(c.1 .0),
                    Cell::new(c.1 .1),
                    Cell::new(c.1 .2),
                ]);
            }
            println!("{table}");
        }
    }
}

#[derive(Subcommand)]
enum ArtifactCommands {
    List,
}

fn do_artifact_cmd(conn: &Connection, cmd: &ArtifactCommands) {
    match cmd {
        ArtifactCommands::List => {
            let (artifacts,): (
                HashMap<String, (String, String, i64, i64, i64, String, String, String)>,
            ) = get_artifact_manager_proxy(conn)
                .method_call("com.snarpix.taneleer.ArtifactManager", "GetArtifacts", ())
                .unwrap();

            let mut table = create_table();
            table.set_header(vec![
                Cell::new("Artifact uuid").add_attribute(Attribute::Bold),
                Cell::new("Class name"),
                Cell::new("Artifact type"),
                Cell::new("Reserve time"),
                Cell::new("Commit time"),
                Cell::new("Use count"),
                Cell::new("State"),
                Cell::new("Next state"),
                Cell::new("Error"),
            ]);

            for (
                uuid,
                (
                    class_name,
                    art_type,
                    reserve_time,
                    commit_time,
                    use_count,
                    state,
                    next_state,
                    error,
                ),
            ) in artifacts
            {
                table.add_row(vec![
                    Cell::new(uuid),
                    Cell::new(class_name),
                    Cell::new(art_type),
                    Cell::new(reserve_time),
                    Cell::new(commit_time),
                    Cell::new(use_count),
                    Cell::new(state),
                    Cell::new(next_state),
                    Cell::new(error),
                ]);
            }
            println!("{table}");
        }
    }
}

#[derive(Subcommand)]
enum SourceCommands {
    List,
}

fn do_source_cmd(conn: &Connection, cmd: &SourceCommands) {
    match cmd {
        SourceCommands::List => {
            let (sources,): (Vec<((String, String), (String, Variant<Box<dyn RefArg>>))>,) =
                get_artifact_manager_proxy(conn)
                    .method_call("com.snarpix.taneleer.ArtifactManager", "GetSources", ())
                    .unwrap();

            let mut table = create_table();
            table.set_header(vec![
                Cell::new("Artifact uuid"),
                Cell::new("Source name"),
                Cell::new("Source type"),
                Cell::new("Source meta"),
            ]);

            for ((artifact_uuid, source_name), (source_type, source_meta)) in sources {
                let meta = match source_type.as_str() {
                    "artifact" => {
                        let mut arg_iter = source_meta.0.as_iter().unwrap();
                        let uuid = arg_iter.next().and_then(|i| i.as_str()).unwrap();
                        uuid.to_owned()
                    }
                    "git" => {
                        let mut arg_iter = source_meta.0.as_iter().unwrap();
                        let url = arg_iter.next().and_then(|i| i.as_str()).unwrap();
                        let commit = arg_iter.next().and_then(|i| i.as_str()).unwrap();
                        format!("{}\n{}", url, commit)
                    }
                    "url" => {
                        let mut arg_iter = source_meta.0.as_iter().unwrap();
                        let url = arg_iter.next().and_then(|i| i.as_str()).unwrap();
                        let hash = arg_iter.next().and_then(|i| i.as_str()).unwrap();
                        format!("{}\n{}", url, hash)
                    }
                    _ => {
                        panic!("Invalid type");
                    }
                };
                table.add_row(vec![
                    Cell::new(artifact_uuid),
                    Cell::new(source_name),
                    Cell::new(source_type),
                    Cell::new(meta),
                ]);
            }
            println!("{table}");
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let conn = Connection::new_session().unwrap();

    match &cli.command {
        MainCommands::Class { cmd } => {
            do_class_cmd(&conn, cmd);
        }
        MainCommands::Artifact { cmd } => {
            do_artifact_cmd(&conn, cmd);
        }
        MainCommands::Source { cmd } => {
            do_source_cmd(&conn, cmd);
        }
    }
}
