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

const UNKNOWN: &str = "<unknown>";

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub fn short_info() -> String {
    format!(
        "{} ({}) ",
        built_info::GIT_VERSION.unwrap_or(UNKNOWN),
        built_info::PROFILE
    )
}

pub fn long_info() -> String {
    format!(
        "version: {} profile: {} rustc: {} target: {} built time: {}",
        built_info::GIT_VERSION.unwrap_or(UNKNOWN),
        built_info::PROFILE,
        built_info::RUSTC_VERSION,
        built_info::TARGET,
        built_info::BUILT_TIME_UTC,
    )
}
