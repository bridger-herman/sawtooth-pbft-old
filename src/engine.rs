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

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use sawtooth_sdk::consensus::{engine::*, service::Service};

use node::PbftNode;

use config;
use timing;

use error::PbftError;

use std::fs::File;
use std::io::prelude::*;

pub struct PbftEngine {
    id: u64,
    death_time: isize,
}

impl PbftEngine {
    pub fn new(id: u64, death_time: isize) -> Self {
        PbftEngine { id: id, death_time: death_time}
    }
}

impl Engine for PbftEngine {
    fn start(
        &mut self,
        updates: Receiver<Update>,
        mut service: Box<Service>,
        chain_head: Block,
        _peers: Vec<PeerInfo>,
    ) {
        // Load on-chain settings
        let config = config::load_pbft_config(chain_head.block_id, &mut service);

        let mut working_ticker = timing::Ticker::new(config.block_duration);
        let mut prev_seconds;
        let mut death_timeout = if self.death_time >= 0 {
            prev_seconds = self.death_time as u64;
            Some(timing::Timeout::new(Duration::from_secs(self.death_time as u64)))
        } else {
            prev_seconds = 0;
            None
        };

        let mut node = PbftNode::new(self.id, &config, service);

        info!("Starting state: {:#?}", node.state);

        let mut mod_file = File::create(format!("state_{}.txt", self.id).as_str()).unwrap();
        mod_file
            .write_all(&format!("{:#?}", node.state).into_bytes())
            .unwrap();

        // Event loop. Keep going until we receive a shutdown message.
        loop {
            let incoming_message = updates.recv_timeout(config.message_timeout);

            if let Some(ref mut timeout) = death_timeout {
                if timeout.is_expired() {
                    panic!("{}: I died", node);
                } else {
                    let remaining = timeout.remaining();
                    if remaining.as_secs() != prev_seconds {
                        prev_seconds = remaining.as_secs();
                        info!("{}: {} seconds until I die", node, prev_seconds);
                    }
                }
            }

            if let Err(e) = match incoming_message {
                Ok(Update::BlockNew(block)) => node.on_block_new(block),
                Ok(Update::BlockValid(block_id)) => node.on_block_valid(block_id),
                Ok(Update::BlockCommit(block_id)) => node.on_block_commit(block_id),
                Ok(Update::PeerMessage(message, _sender_id)) => node.on_peer_message(message),
                Ok(Update::Shutdown) => break,
                Err(RecvTimeoutError::Timeout) => Err(PbftError::Timeout),
                Err(RecvTimeoutError::Disconnected) => {
                    error!("Disconnected from validator");
                    break;
                }
                _ => Ok(unimplemented!()),
            } {
                // Do nothing for Timeout errors
                match e {
                    PbftError::Timeout => (),
                    _ => error!("{}", e),
                }
            }

            working_ticker.tick(|| {
                if let Err(e) = node.update_working_block() {
                    error!("{}", e);
                }
            });

            // Check to see if timeout has expired; initiate ViewChange if necessary
            if node.check_timeout_expired() {
                node.start_view_change()
                    .unwrap_or_else(|e| error!("Couldn't start view change: {}", e));
            }

            if node.msg_log.at_checkpoint() {
                node.start_checkpoint()
                    .unwrap_or_else(|e| error!("Couldn't start checkpoint: {}", e));
            }

            node.retry_unread()
                .unwrap_or_else(|e| error!("Couldn't retry unread: {}", e));
        }
    }

    fn version(&self) -> String {
        String::from("0.1")
    }

    fn name(&self) -> String {
        String::from("sawtooth-pbft")
    }
}
