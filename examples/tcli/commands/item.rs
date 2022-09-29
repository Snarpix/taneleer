use clap::Subcommand;
use comfy_table::{Cell, Table};
use taneleer::api::*;

use crate::{
    connection::{Connection, MethodCall},
    util::create_table,
};

#[derive(Subcommand)]
pub enum ItemCommands {
    List,
}

pub fn create_item_table() -> Table {
    let mut table = create_table();
    table.set_header(vec![
        Cell::new("Artifact uuid"),
        Cell::new("Item id"),
        Cell::new("Item size"),
        Cell::new("Item hash"),
    ]);
    table
}

pub fn print_item(a: &ArtifactItem, t: &mut Table) {
    let ArtifactItem {
        uuid,
        info: ArtifactItemInfo { id, hash, size },
    } = a;
    let Hashsum::Sha256(hash) = hash;
    t.add_row(vec![
        Cell::new(uuid),
        Cell::new(id),
        Cell::new(hex::encode(&hash)),
        Cell::new(size),
    ]);
}

pub fn do_item_cmd(conn: &mut Connection, cmd: &ItemCommands) {
    match cmd {
        ItemCommands::List => {
            let items = conn.get_items();

            let mut table = create_item_table();

            for a in &items {
                print_item(a, &mut table);
            }
            println!("{table}");
        }
    }
}
