#![allow(unused_imports)]
#![allow(dead_code)]
use crate::cliargs::CliAction;
use clap::{Parser, Subcommand};

mod branches;
mod cliargs;
mod layers;
mod types;
mod usgs;
mod utils;

#[derive(Parser)]
struct Cli {
    /// Don't print the stderr outputs
    #[arg(short, long, action)]
    quiet: bool,
    /// Command to run
    #[command(subcommand)]
    action: Action,
}

/// generate the subcommands using the module, command name and docstring.
macro_rules! subcommands{
    { $( $(#[doc = $doc:expr])* $mod:ident $cmd:ident ),*$(,)? } => {
	#[derive(Subcommand)]
	enum Action {
	    $( $(#[doc=$doc])*
		 $cmd($mod::CliArgs),
	    )*
	}

	impl CliAction for Action {
	    fn run(self) -> anyhow::Result<()> {
		match self {
		    $(
			Self::$cmd(v) => v.run(),
		    )*
		}
	    }
	}

    }
}

// case change in macro would have made it nice, but we'll just repeat
// it, other than that, perfect
subcommands! {
    /// Download data from USGS NHD+
    usgs Usgs,
    /// Show list of layers in a GIS file
    layers Layers,
    /// Detect Branches in the stream network
    ///
    /// The command will list the count of different types of
    /// nodes. If output is given, it'll save the output GIS file with
    /// branch type and the location
    branches Branches,
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    args.action.run()
}
