use clap::Subcommand;
use comfy_table::{Cell, Table};
use taneleer::api::*;
use uuid::Uuid;

use crate::{
    connection::{Connection, MethodCall},
    util::create_table,
};

#[derive(Subcommand)]
pub enum SourceCommands {
    List,
}

pub fn create_source_table() -> Table {
    let mut table = create_table();
    table.set_header(vec![
        Cell::new("Artifact uuid"),
        Cell::new("Source name"),
        Cell::new("Source type"),
        Cell::new("Source meta"),
    ]);
    table
}

pub fn print_source(artifact_uuid: &Uuid, a: &Source, t: &mut Table) {
    let Source { name, source } = a;
    let meta = match source {
        SourceType::Artifact { uuid } => uuid.to_string(),
        SourceType::Git { repo, commit } => {
            let commit = hex::encode(commit);
            format!("{}\n{}", repo, commit)
        }
        SourceType::Url { url, hash } => {
            let Hashsum::Sha256(hash) = hash;
            let hash = hex::encode(hash);
            format!("{}\n{}", url, hash)
        }
    };
    t.add_row(vec![
        Cell::new(artifact_uuid),
        Cell::new(name),
        Cell::new::<&str>(source.into()),
        Cell::new(meta),
    ]);
}

pub fn do_source_cmd(conn: &mut Connection, cmd: &SourceCommands) {
    match cmd {
        SourceCommands::List => {
            let sources = conn.get_sources();
            let mut table = create_source_table();
            for (artifact_uuid, s) in &sources {
                print_source(artifact_uuid, s, &mut table);
            }
            println!("{table}");
        }
    }
}
