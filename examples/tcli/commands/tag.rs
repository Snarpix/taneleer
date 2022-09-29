use clap::Subcommand;
use comfy_table::{Cell, Table};
use taneleer::api::*;

use crate::{
    connection::{Connection, MethodCall},
    util::create_table,
};

#[derive(Subcommand)]
pub enum TagCommands {
    List,
}

pub fn create_tag_table() -> Table {
    let mut table = create_table();
    table.set_header(vec![
        Cell::new("Tag"),
        Cell::new("Tag value"),
        Cell::new("Artifact uuid"),
    ]);
    table
}

pub fn print_tag(a: &ArtifactTag, t: &mut Table) {
    let ArtifactTag {
        artifact_uuid,
        tag: Tag { name, value },
    } = a;
    t.add_row(vec![
        Cell::new(name),
        Cell::new(value.as_deref().unwrap_or("NULL")),
        Cell::new(artifact_uuid),
    ]);
}

pub fn do_tag_cmd(conn: &mut Connection, cmd: &TagCommands) {
    match cmd {
        TagCommands::List => {
            let items = conn.get_tags();

            let mut table = create_tag_table();

            for a in items {
                print_tag(&a, &mut table);
            }
            println!("{table}");
        }
    }
}
