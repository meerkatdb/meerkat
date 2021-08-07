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

use tracing::{info, Level, span};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Debug)]
pub struct Config {
    pub db_path: PathBuf,
    pub gossip_port: u16,
    pub seeds: Vec<String>,
}

pub struct Meerkat {
    conf: Config,
}

impl Meerkat
{
    pub fn new(conf: Config) -> Self {
        Self { conf }
    }

    pub fn start(&mut self) {
        self.init_logging();
        let span = span!(Level::TRACE, "meerkat");
        let _guard = span.enter();
        info!("starting");
    }

    pub fn stop(&mut self) {
        let span = span!(Level::TRACE, "meerkat");
        let _guard = span.enter();
        info!("stopping");
    }

    fn init_logging(&self) {
        let formatter = tracing_subscriber::fmt::Layer::default().with_ansi(true);
        let subscriber = tracing_subscriber::registry::Registry::default();
        let subscriber = subscriber.with(formatter);
        let _ = LogTracer::init();
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting tracing default failed");
    }
}




