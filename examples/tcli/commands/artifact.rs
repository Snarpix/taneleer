use clap::Subcommand;
use comfy_table::{Attribute, Cell, Table};
use taneleer::api::*;

use crate::{
    commands::{
        item::{create_item_table, print_item},
        source::{create_source_table, print_source},
        tag::{create_tag_table, print_tag},
        usage::{create_usage_table, print_usage},
    },
    connection::{Connection, MethodCall},
    parse::{parse_srcs, parse_tags},
    util::create_table,
};

#[derive(Subcommand)]
pub enum ArtifactCommands {
    List,
    Reserve {
        class_name: String,
        #[clap(long)]
        src: Vec<String>,
        #[clap(long)]
        tag: Vec<String>,
    },
    Abort {
        artifact_uuid: String,
    },
    Commit {
        artifact_uuid: String,
        #[clap(long)]
        tag: Vec<String>,
    },
    Get {
        artifact_uuid: String,
    },
    Use {
        artifact_uuid: String,
    },
    Last {
        class_name: String,
        #[clap(long)]
        src: Vec<String>,
        #[clap(long)]
        tag: Vec<String>,
        #[clap(long)]
        no_use: bool,
    },
}

fn create_artifact_table() -> Table {
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
    table
}

fn print_artifact(a: Artifact, t: &mut Table) {
    t.add_row(vec![
        Cell::new(a.uuid),
        Cell::new(a.data.class_name),
        Cell::new(a.data.art_type),
        Cell::new(a.data.reserve_time),
        Cell::new(
            a.data
                .commit_time
                .map(|i| i64::to_string(&i))
                .unwrap_or_else(|| "NULL".to_owned()),
        ),
        Cell::new(a.data.use_count),
        Cell::new(<&str>::from(a.data.state)),
        Cell::new(a.data.next_state.map(<&str>::from).unwrap_or("NULL")),
        Cell::new(a.data.error.unwrap_or_else(|| "NULL".to_owned())),
    ]);
}

pub fn do_artifact_cmd(conn: &mut Connection, cmd: &ArtifactCommands) {
    match cmd {
        ArtifactCommands::List => {
            let mut table = create_artifact_table();
            for a in conn.get_artifacts() {
                print_artifact(a, &mut table);
            }
            println!("{table}");
        }
        ArtifactCommands::Reserve {
            class_name,
            src,
            tag,
        } => {
            let class_name = class_name.clone();
            let sources = parse_srcs(src);
            let tags = parse_tags(tag);
            let res = conn.reserve_artifact(ReserveArtifact {
                class_name,
                sources,
                tags,
            });
            println!("{}", res.uuid);
            println!("{}", res.url);
        }
        ArtifactCommands::Abort { artifact_uuid } => {
            let uuid = uuid::Uuid::parse_str(artifact_uuid).unwrap();
            let () = conn.abort_reserve(AbortReserve { uuid });
        }
        ArtifactCommands::Commit { artifact_uuid, tag } => {
            let uuid = uuid::Uuid::parse_str(artifact_uuid).unwrap();
            let tags = parse_tags(tag);
            let () = conn.commit_artifact(CommitArtifact { uuid, tags });
        }
        ArtifactCommands::Get { artifact_uuid } => {
            let uuid = uuid::Uuid::parse_str(artifact_uuid).unwrap();
            let res = conn.get_artifact(GetArtifact { uuid });
            {
                let mut table = create_artifact_table();
                print_artifact(
                    Artifact {
                        uuid,
                        data: res.info,
                    },
                    &mut table,
                );
                println!("{}", table);
            }
            {
                let mut table = create_source_table();
                for s in &res.sources {
                    print_source(&uuid, s, &mut table);
                }
                println!("{}", table);
            }
            {
                let mut table = create_tag_table();
                for s in res.tags {
                    print_tag(
                        &ArtifactTag {
                            artifact_uuid: uuid,
                            tag: s,
                        },
                        &mut table,
                    );
                }
                println!("{}", table);
            }
            {
                let mut table = create_item_table();
                for s in res.items {
                    print_item(&ArtifactItem { uuid, info: s }, &mut table);
                }
                println!("{}", table);
            }
            {
                let mut table = create_usage_table();
                for s in res.usages {
                    print_usage(
                        &ArtifactUsage {
                            artifact_uuid: uuid,
                            usage: s,
                        },
                        &mut table,
                    );
                }
                println!("{}", table);
            }
        }
        ArtifactCommands::Use {
            artifact_uuid,
        } => {
            let uuid = uuid::Uuid::parse_str(artifact_uuid).unwrap();
            let res = conn.use_artifact(UseArtifact { uuid });
            println!("{}", res.uuid);
            println!("{}", res.url);
        }
        ArtifactCommands::Last {
            class_name,
            src,
            tag,
            no_use,
        } => {
            let class_name = class_name.clone();
            let tags = parse_tags(tag);
            let sources = parse_srcs(src);
            if *no_use {
                let res = conn.find_last_artifact(FindLastArtifact {
                    class_name,
                    sources,
                    tags,
                });
                println!("{}", res.uuid);
            } else {
                let res = conn.use_last_artifact(UseLastArtifact {
                    class_name,
                    sources,
                    tags,
                });
                println!("{}", res.usage_uuid);
                println!("{}", res.artifact_uuid);
                println!("{}", res.url);
            }
        }
    }
}
