#![allow(clippy::let_unit_value)]

mod commands;
mod connection;
mod parse;
mod util;

use clap::{Parser, Subcommand};
use connection::MethodCall;

use self::connection::Connection;
use commands::{artifact::*, class::*, item::*, source::*, tag::*, usage::*};

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

fn main() {
    let cli = Cli::parse();

    let mut conn = Connection::new();

    match &cli.command {
        MainCommands::Class { cmd } => {
            do_class_cmd(&mut conn, cmd);
        }
        MainCommands::Artifact { cmd } => {
            do_artifact_cmd(&mut conn, cmd);
        }
        MainCommands::Source { cmd } => {
            do_source_cmd(&mut conn, cmd);
        }
        MainCommands::Item { cmd } => {
            do_item_cmd(&mut conn, cmd);
        }
        MainCommands::Tag { cmd } => {
            do_tag_cmd(&mut conn, cmd);
        }
        MainCommands::Usage { cmd } => {
            do_usage_cmd(&mut conn, cmd);
        }
    }

    conn.close();
}
