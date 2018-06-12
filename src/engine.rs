/*
 * Copyright 2018 Bitwise IO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

use sawtooth_sdk::consensus::{engine::*, service::Service};
use std::sync::mpsc::{Receiver, RecvTimeoutError};

pub struct PbftEngine {
    exit: Exit,
}

impl PbftEngine {
    pub fn new() -> Self {
        PbftEngine {
            exit: Exit::new(),
        }
    }
}

impl Engine for PbftEngine {
    fn start(
        &self,
        update: Receiver<Update>,
        service: Box<Service>,
        mut chain_head: Block,
        _peers: Vec<PeerInfo>,
    ) {

    }

    fn stop(&self) {
        self.exit.set();
    }

    fn version(&self) -> String {
        String::from("0.1")
    }

    fn name(&self) -> String {
        String::from("PBFT")
    }
}
