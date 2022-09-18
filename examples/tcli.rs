use std::{collections::HashMap, time::Duration};

use clap::{builder::ArgAction, ArgMatches, Parser, Subcommand};
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

fn get_artifact_class_proxy<'a>(
    conn: &'a Connection,
    class_name: &str,
) -> Proxy<'a, &'a Connection> {
    conn.with_proxy(
        "com.snarpix.taneleer",
        format!("/com/snarpix/taneleer/Classes/{}", class_name),
        Duration::from_millis(5000),
    )
}

fn get_artifact_reserve_proxy<'a>(
    conn: &'a Connection,
    artifact_uuid: &str,
) -> Proxy<'a, &'a Connection> {
    conn.with_proxy(
        "com.snarpix.taneleer",
        format!(
            "/com/snarpix/taneleer/Artifacts/{}",
            artifact_uuid.to_string().replace('-', "_")
        ),
        Duration::from_millis(5000),
    )
}

fn get_artifact_proxy<'a>(conn: &'a Connection, artifact_uuid: &str) -> Proxy<'a, &'a Connection> {
    conn.with_proxy(
        "com.snarpix.taneleer",
        format!(
            "/com/snarpix/taneleer/Artifacts/{}",
            artifact_uuid.to_string().replace('-', "_")
        ),
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
    Item {
        #[clap(subcommand)]
        cmd: ItemCommands,
    },
    Tag {
        #[clap(subcommand)]
        cmd: TagCommands,
    },
    Usage {
        #[clap(subcommand)]
        cmd: UsageCommands,
    },
}

#[derive(Subcommand)]
enum ClassCommands {
    List,
    Create {
        #[clap(value_parser)]
        name: String,
        #[clap(value_parser)]
        backend_name: String,
        #[clap(value_parser)]
        art_type: String,
    },
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
        ClassCommands::Create {
            name,
            backend_name,
            art_type,
        } => {
            let () = get_artifact_manager_proxy(conn)
                .method_call(
                    "com.snarpix.taneleer.ArtifactManager",
                    "CreateArtifactClass",
                    (name, backend_name, art_type),
                )
                .unwrap();
        }
    }
}

#[derive(Subcommand)]
enum ArtifactCommands {
    List,
    Reserve {
        class_name: String,
        #[clap(long)]
        proxy: Option<String>,
        #[clap(long)]
        src: Vec<String>,
        #[clap(long)]
        tag: Vec<String>,
    },
    Commit {
        artifact_uuid: String,
        #[clap(long)]
        tag: Vec<String>,
    },
    Use {
        artifact_uuid: String,
        #[clap(long)]
        proxy: Option<String>,
    },
}

fn parse_tags(tag: &Vec<String>) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    for t in tag {
        let mut t = t.split(':');
        let tag = t.next().unwrap();
        let value = t.next().unwrap_or("");
        tags.push((tag.to_owned(), value.to_owned()));
    }
    tags
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
        ArtifactCommands::Reserve {
            class_name,
            proxy,
            src,
            tag,
        } => {
            let mut srcs: HashMap<String, (String, Variant<Box<dyn RefArg>>)> = HashMap::new();
            for s in src {
                let mut s = s.split(',');
                let name = s.next().unwrap();
                let src_type = s.next().unwrap();
                match src_type {
                    "url" | "git" => {
                        let url = s.next().unwrap();
                        let hash = s.next().unwrap();
                        srcs.insert(
                            name.to_owned(),
                            (
                                src_type.to_owned(),
                                Variant(Box::new((url.to_owned(), hash.to_owned()))),
                            ),
                        );
                    }
                    "artifact" => {
                        let uuid = s.next().unwrap();
                        srcs.insert(
                            name.to_owned(),
                            (src_type.to_owned(), Variant(Box::new(uuid.to_owned()))),
                        );
                    }
                    _ => {
                        panic!("Invalid src type: {}", src_type);
                    }
                }
            }
            let tags = parse_tags(tag);
            let (uuid, url): (String, String) = get_artifact_class_proxy(conn, &class_name)
                .method_call(
                    "com.snarpix.taneleer.ArtifactClass",
                    "Reserve",
                    (proxy.as_deref().unwrap_or(""), srcs, tags),
                )
                .unwrap();
            println!("{}", uuid);
            println!("{}", url);
        }
        ArtifactCommands::Commit { artifact_uuid, tag } => {
            let tags = parse_tags(tag);
            let () = get_artifact_reserve_proxy(conn, &artifact_uuid)
                .method_call("com.snarpix.taneleer.ArtifactReserve", "Commit", (tags,))
                .unwrap();
        }
        ArtifactCommands::Use {
            artifact_uuid,
            proxy,
        } => {
            let () = get_artifact_proxy(conn, &artifact_uuid)
                .method_call(
                    "com.snarpix.taneleer.Artifact",
                    "Get",
                    (proxy.as_deref().unwrap_or(""),),
                )
                .unwrap();
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

#[derive(Subcommand)]
enum ItemCommands {
    List,
}

fn do_item_cmd(conn: &Connection, cmd: &ItemCommands) {
    match cmd {
        ItemCommands::List => {
            let (items,): (Vec<((String, String), (u64, String))>,) =
                get_artifact_manager_proxy(conn)
                    .method_call("com.snarpix.taneleer.ArtifactManager", "GetItems", ())
                    .unwrap();

            let mut table = create_table();
            table.set_header(vec![
                Cell::new("Artifact uuid"),
                Cell::new("Item id"),
                Cell::new("Item size"),
                Cell::new("Item hash"),
            ]);

            for ((artifact_uuid, item_id), (item_size, item_hash)) in items {
                table.add_row(vec![
                    Cell::new(artifact_uuid),
                    Cell::new(item_id),
                    Cell::new(item_size),
                    Cell::new(item_hash),
                ]);
            }
            println!("{table}");
        }
    }
}

#[derive(Subcommand)]
enum TagCommands {
    List,
}

fn do_tag_cmd(conn: &Connection, cmd: &TagCommands) {
    match cmd {
        TagCommands::List => {
            let (items,): (Vec<((String, String), String)>,) = get_artifact_manager_proxy(conn)
                .method_call("com.snarpix.taneleer.ArtifactManager", "GetTags", ())
                .unwrap();

            let mut table = create_table();
            table.set_header(vec![
                Cell::new("Tag"),
                Cell::new("Tag value"),
                Cell::new("Artifact uuid"),
            ]);

            for ((tag_name, tag_value), artifact_uuid) in items {
                table.add_row(vec![
                    Cell::new(tag_name),
                    Cell::new(tag_value),
                    Cell::new(artifact_uuid),
                ]);
            }
            println!("{table}");
        }
    }
}

#[derive(Subcommand)]
enum UsageCommands {
    List,
}

fn do_usage_cmd(conn: &Connection, cmd: &UsageCommands) {
    match cmd {
        UsageCommands::List => {
            let (items,): (Vec<(String, (String, String))>,) = get_artifact_manager_proxy(conn)
                .method_call("com.snarpix.taneleer.ArtifactManager", "GetUsages", ())
                .unwrap();

            let mut table = create_table();
            table.set_header(vec![
                Cell::new("Usage uuid"),
                Cell::new("Artifact uuid"),
                Cell::new("Reserve time"),
            ]);

            for (uuid, (artifact_uuid, reserve_time)) in items {
                table.add_row(vec![
                    Cell::new(uuid),
                    Cell::new(artifact_uuid),
                    Cell::new(reserve_time),
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
        MainCommands::Item { cmd } => {
            do_item_cmd(&conn, cmd);
        }
        MainCommands::Tag { cmd } => {
            do_tag_cmd(&conn, cmd);
        }
        MainCommands::Usage { cmd } => {
            do_usage_cmd(&conn, cmd);
        }
    }
}
