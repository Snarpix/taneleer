use clap::Subcommand;
use comfy_table::{Attribute, Cell};
use taneleer::api::*;

use crate::{
    connection::{Connection, MethodCall},
    util::create_table,
};

#[derive(Subcommand)]
pub enum ClassCommands {
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

pub fn do_class_cmd(conn: &mut Connection, cmd: &ClassCommands) {
    match cmd {
        ClassCommands::List => {
            let classes = conn.get_classes();

            let mut table = create_table();
            table.set_header(vec![
                Cell::new("Class name").add_attribute(Attribute::Bold),
                Cell::new("Backend name"),
                Cell::new("Artifact type"),
                Cell::new("State"),
            ]);

            for c in classes {
                table.add_row(vec![
                    Cell::new(c.name),
                    Cell::new(c.data.backend_name),
                    Cell::new(c.data.art_type),
                    Cell::new(c.state),
                ]);
            }
            println!("{table}");
        }
        ClassCommands::Create {
            name,
            backend_name,
            art_type,
        } => {
            let name = name.to_owned();
            let backend_name = backend_name.to_owned();
            let art_type: ArtifactType = art_type.parse().unwrap();
            let () = conn.create_artifact_class(CreateArtifactClass {
                name,
                backend_name,
                art_type,
            });
        }
    }
}
