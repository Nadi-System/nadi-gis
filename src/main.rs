use crate::cliargs::CliAction;
use clap::{Parser, Subcommand};

mod cliargs;
mod usgs;

#[derive(Parser)]
struct Cli {
    /// Don't print the stderr outputs
    #[arg(short, long, action)]
    quiet: bool,
    /// Command to run
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Download data from USGS NHD+
    Usgs(usgs::CliArgs),
}

impl CliAction for Action {
    fn run(self) -> anyhow::Result<()> {
        match self {
            Self::Usgs(v) => v.run(),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    args.action.run()
}
