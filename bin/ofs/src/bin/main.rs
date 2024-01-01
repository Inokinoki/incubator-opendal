// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use clap::Parser;
use opendal::{Operator, Scheme};
use std::{collections::HashMap, str::FromStr};
use tokio;

use fuse3::path::Session;
use fuse3::MountOptions;

use ofs::Ofs;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct OfsApp {
    pub mount_point: String,

    /// OpenDAL scheme
    #[arg(short, long, value_parser = parse_type)]
    pub r#type: Scheme,

    /// Configuration of the OpenDAL scheme in the format <key1>=<val1>,<key2>=<val2>,..
    #[arg(short, long, value_parser = parse_options)]
    pub options: Option<HashMap<String, String>>,
}

fn parse_options(raw: &str) -> Result<HashMap<String, String>, String> {
    raw.split(',')
        .map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .ok_or(String::from("Invalid key value format".to_string()))
        })
        .collect::<Result<HashMap<String, String>, String>>()
}

fn parse_type(raw: &str) -> Result<Scheme, String> {
    Scheme::from_str(raw).map_err(|_| "Invalid OpenDAL scheme".to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = OfsApp::parse();
    env_logger::init();

    let options = config.options.unwrap_or_default();

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut mount_options = MountOptions::default();
    mount_options.uid(uid).gid(gid);

    let mount_path = config.mount_point;
    Session::new(mount_options)
        .mount_with_unprivileged(
            Ofs {
                op: Operator::via_map(config.r#type, options).unwrap(),
            },
            mount_path,
        )
        .await
        .unwrap()
        .await
        .unwrap();
}
