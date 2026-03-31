use anyhow::Result;
use clap::Parser;
use fast_dump::app;
use fast_dump::cli::Cli;

fn main() -> Result<()> {
    app::run(Cli::parse())
}
