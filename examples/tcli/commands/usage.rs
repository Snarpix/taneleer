use clap::Subcommand;
use comfy_table::{Cell, Table};
use taneleer::api::*;

use crate::{
    connection::{Connection, MethodCall},
    util::create_table,
};

#[derive(Subcommand)]
pub enum UsageCommands {
    List,
}

pub fn create_usage_table() -> Table {
    let mut table = create_table();
    table.set_header(vec![
        Cell::new("Usage uuid"),
        Cell::new("Artifact uuid"),
        Cell::new("Reserve time"),
    ]);
    table
}

pub fn print_usage(a: &ArtifactUsage, t: &mut Table) {
    let ArtifactUsage {
        artifact_uuid,
        usage: Usage { uuid, reserve_time },
    } = a;
    t.add_row(vec![
        Cell::new(uuid),
        Cell::new(artifact_uuid),
        Cell::new(reserve_time),
    ]);
}

pub fn do_usage_cmd(conn: &mut Connection, cmd: &UsageCommands) {
    match cmd {
        UsageCommands::List => {
            let items = conn.get_usages();

            let mut table = create_usage_table();

            for a in items {
                print_usage(&a, &mut table);
            }
            println!("{table}");
        }
    }
}
