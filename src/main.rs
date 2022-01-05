// Copyright 2021 The Meerkat Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;

use structopt::clap::AppSettings;
use structopt::StructOpt;

use server::Config;

mod build;
mod server;
mod store;

#[derive(Debug, StructOpt)]
struct Args {
    /// The directory where MeerkatDB stores its data
    #[structopt(long, env = "MK_DB_PATH", parse(from_os_str))]
    db_path: PathBuf,
    /// The gossip port.
    #[structopt(long, default_value = "7775", env = "MK_GOSSIP_PORT")]
    gossip_port: u16,
    /// Comma-separated list of public IP addresses or hostnames
    /// used for bootstrapping new nodes.
    #[structopt(long, env = "MK_SEEDS")]
    seeds: Option<String>,
}

const NO_VERSION_HELP_TEMPLATE: &str = "
USAGE:
    {usage}

{all-args}";

fn main() {
    println!("meerkat {}", build::short_info());

    let long_info = build::long_info();

    let clap = Args::clap()
        .version(long_info.as_ref())
        .global_setting(AppSettings::UnifiedHelpMessage)
        .template(NO_VERSION_HELP_TEMPLATE);

    let args = Args::from_clap(&clap.get_matches());

    let conf = Config {
        db_path: args.db_path,
        gossip_port: args.gossip_port,
        seeds: args
            .seeds
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect(),
    };

    let mut server = server::Meerkat::new(conf);

    // TODO(gvelo): add panic handler, signal handlers, etc.

    server.start();
}
