#![allow(unused_imports)]
#![allow(dead_code)]
use crate::cliargs::CliAction;
use clap::{Parser, Subcommand};

mod cliargs;
mod types;
mod utils;

/// Generate the subcommands using the module, command name and docstring.
///
/// The macro will load the mod, and use the CliArgs defined in the
/// mod to define the command. It will also forward the doc strings to
/// the corresponding commands so that they can be accessed from help.
macro_rules! subcommands{
    { $( $(#[doc = $doc:expr])* $mod:ident $cmd:ident ),*$(,)? } => {
	$(
	    mod $mod;
	)*

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
    ///
    /// This is useful to peek into what a GIS file has, so you can
    /// pass that layer as a input file to other commands.
    layers Layers,
    /// Check the stream network to see outlet, branches, etc
    ///
    /// The command will list the count of different types of
    /// nodes. If output is given, it'll save the output GIS file with
    /// branch type and the location. To use a streams file for
    /// network generation make sure it has only one outlet, and no
    /// branches. If it has zero outlet, and same number of branches
    /// and confluences, then it is not a streams file but a list of
    /// points.
    check Check,
    /// Order the streams, adds order attribute to each segment
    ///
    /// Use valid streams file for good results. If the streams has
    /// points, it'll error out, if it has branches, then only the
    /// main branch will get the upstream stream order, other branches
    /// will start from 0.
    order Order,
    /// Find the network information from streams file between points
    network Network,
}

#[derive(Parser)]
struct Cli {
    /// Don't print the stderr outputs
    #[arg(short, long, action)]
    quiet: bool,
    /// Command to run
    #[command(subcommand)]
    action: Action,
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    args.action.run()
}
