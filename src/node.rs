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

use protobuf;
use protobuf::Message;

use std::collections::HashMap;

use std::fmt;

use pbft_log::PbftLog;
use sawtooth_sdk::consensus::service::Service;
use sawtooth_sdk::consensus::engine::{
    PeerMessage,
    Block,
    BlockId,
    PeerId,
    Error,
};

use protos::pbft_message::{
    PbftBlock,
    PbftMessage,
    PbftMessageInfo,
};

// Possible roles for a node
// Primary is the only node who is allowed to commit to the blockchain
#[derive(PartialEq)]
enum PbftNodeRole {
    Primary,
    Secondary,
}


// Stages of the PBFT algorithm
#[derive(Debug, PartialEq)]
enum PbftStage {
    Unset,
    NotStarted,
    PrePreparing,
    Preparing,
    Committing,
    FinalCommitting,
    Finished,
}


// The actual node
pub struct PbftNode {
    id: u64,
    stage: PbftStage,
    service: Box<Service>,
    role: PbftNodeRole,
    network_node_ids: HashMap<u64, PeerId>,
    msg_log: PbftLog,
}

impl fmt::Display for PbftNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ast = if self.role == PbftNodeRole::Primary {
            "*"
        } else {
            " "
        };

        write!(f, "Node {}{:02}", ast, self.id)
    }
}

impl PbftNode {
    pub fn new(id: u64, peers: HashMap<PeerId, u64>, mut service: Box<Service>) -> Self {
        // service.initialize_block(None).expect("Failed to initialize block");

        // TODO: This is inefficient, but should only get run as many times as there are nodes
        let peer_id_map: HashMap<u64, PeerId> = peers
            .clone()
            .into_iter()
            .map(|(peer_id, node_id)| (node_id, peer_id))
            .collect();

        let current_primary = peers
            .iter()
            .map(|(_peer_id, node_id)| node_id)
            .min()
            .unwrap_or(&1);

        PbftNode {
            id: id,
            stage: PbftStage::NotStarted,
            role: if &id == current_primary {
                PbftNodeRole::Primary
            } else {
                PbftNodeRole::Secondary
            },
            network_node_ids: peer_id_map,
            service: service,
            msg_log: PbftLog::new(),
        }
    }

    // Handle a peer message from another PbftNode
    // This method controls the PBFT multicast protocol (PrePrepare, Prepare, Commit, CommitFinal).
    pub fn on_peer_message(&mut self, msg: PeerMessage) {

        let msg_type = msg.message_type.as_str();

        match msg_type {
            "pre_prepare" | "prepare" | "commit" | "commit_final" => {
                // Don't process message if we're not ready for it.
                // i.e. Don't process prepare messages if we're not in the PbftStage::Preparing
                if !self._check_ready_for_msg(msg.message_type.as_str()) {
                    return;
                }

                let deser_msg = protobuf::parse_from_bytes::<PbftMessage>(&msg.content)
                    .unwrap_or_else(|err| {
                        error!("Couldn't deserialize message: {}", err);
                        panic!();
                });

                info!(
                    "{}: received PeerMessage: {:?} from node {:?}",
                    self,
                    msg.message_type,
                    self._get_node_id(PeerId::from(deser_msg.get_block().clone().signer_id))
                );

                // TODO: better way than nested matches?
                match msg_type {
                    "pre_prepare" => {
                        // TODO check legitimacy of pre_prepare messages

                        let msg_bytes = make_msg_bytes(
                            make_msg_info("prepare", 1, 1),
                            (*deser_msg.get_block()).clone()
                        );

                        self.service.broadcast("prepare", msg_bytes)
                            .unwrap_or_else(|err| error!("Couldn't broadcast: {}", err));
                        debug!("{}: sending {:?}", self, "prepare");
                        self.stage = PbftStage::Preparing;
                    }
                    "prepare" => {
                        // TODO check prepared predicate

                        self.service.check_blocks(vec![BlockId::from(deser_msg.get_block().clone().block_id)])
                            .expect("Failed to check blocks");
                    }
                    "commit" => {
                        // TODO: check committed predicate

                        let msg_bytes = make_msg_bytes(
                            make_msg_info("commit_final", 1, 1),
                            (*deser_msg.get_block()).clone()
                        );

                        self.service.broadcast("commit_final", msg_bytes)
                            .unwrap_or_else(|err| error!("Couldn't broadcast: {}", err));
                        debug!("{}: sending {:?}", self, "commit_final");
                        self.stage = PbftStage::FinalCommitting;
                    }
                    "commit_final" => {
                        if self.role == PbftNodeRole::Primary {
                            self.service.commit_block(
                                    BlockId::from(deser_msg .get_block().block_id.clone())
                                )
                                .expect("Failed to commit block");
                        }

                        self.stage = PbftStage::Finished;
                    }
                    _ => unimplemented!(),
                }
            }
            t => warn!("Message type {:?} not implemented", t),
        }
    }

    // Handle a new block from the Validator
    // Create a new working block on the working block queue and kick off the consensus algorithm
    // by broadcasting a "pre_prepare" message to peers
    pub fn on_block_new(&mut self, block: Block) {
        info!("{}: received BlockNew: {:?}", self, block.block_id);
        // self.service.check_blocks(vec![block.block_id.clone()])
            // .expect("Failed to check blocks");

        // TODO: Check validity of block
        if self.role == PbftNodeRole::Primary {
            // TODO: keep track of seq number and view in Node
            let msg_bytes = make_msg_bytes(
                make_msg_info("pre_prepare", 1, 1),
                pbft_block_from_block(block)
            );

            self.service.broadcast("pre_prepare", msg_bytes)
                .unwrap_or_else(|err| error!("Couldn't broadcast: {}", err));
            debug!("{}: sending {:?}", self, "pre_prepare");
            self.stage = PbftStage::PrePreparing;
        }
    }

    // Handle a block commit from the Validator
    // If we're a primary, do nothing??
    // If we're a secondary, commit the block in the message
    pub fn on_block_commit(&mut self, block_id: BlockId) {
        info!("{}: received BlockCommit: {:?}", self, block_id);

        match self.role {
            PbftNodeRole::Primary => {
                // do nothing
            }
            PbftNodeRole::Secondary => {
                self.service.commit_block(block_id)
                    .unwrap_or_else(|err| error!("Couldn't commit block: {}", err));
            }
        }

        // self.service.initialize_block(None).expect("Failed to initialize block");
    }

    // Handle a valid block notice
    // This message comes after check_blocks is called
    pub fn on_block_valid(&mut self, block_id: BlockId) {
        info!("{}: received BlockValid: {:?}", self, block_id);

        // TODO: remove panic?
        let valid_blocks: Vec<Block> = self.service.get_blocks(vec![block_id])
            .unwrap_or_else(|err| panic!("Couldn't get block: {:?}", err))
            .into_iter()
            .map(|(_block_id, block)| block)
            .collect();

        assert_eq!(valid_blocks.len(), 1);

        let msg_bytes = make_msg_bytes(
            make_msg_info("commit", 1, 1),
            pbft_block_from_block(valid_blocks[0].clone())
        );

        self.service.broadcast("commit", msg_bytes)
            .unwrap_or_else(|err| error!("Couldn't broadcast: {}", err));
        debug!("{}: sending {:?}", self, "commit");
        self.stage = PbftStage::Committing;
    }

    pub fn update_working_block(&mut self) {
        match self.service.finalize_block(vec![]) {
            Ok(block_id) => {
                debug!("{}: Publishing block {:?}", self, block_id);
                self.service.initialize_block(None)
                    .unwrap_or_else(|err| error!("Couldn't initialize block: {}", err));
            },
            Err(Error::BlockNotReady) => {
                debug!("{}: Block not ready", self);
            },
            Err(err) => panic!("Failed to finalize block: {:?}", err),
        }
    }

    // Checks to see if a message type is acceptable to receive, in this node's
    // current stage.
    fn _check_ready_for_msg(&self, msg_type: &str) -> bool {
        let corresponding_stage = match msg_type {
            "pre_prepare" => PbftStage::NotStarted,
            "prepare" => PbftStage::PrePreparing,
            "commit" => PbftStage::Preparing,
            "commit_final" => PbftStage::Committing,
            _ => {
                warn!("Didn't find a PbftStage corresponding to {}", msg_type);
                return true;
            }
        };

        corresponding_stage == self.stage
    }

    // Obtain the node ID (u64) from a PeerId
    fn _get_node_id(&self, peer_id: PeerId) -> u64 {
        let matching_node_ids: Vec<u64> = self.network_node_ids
            .iter()
            .filter(|(_node_id, network_peer_id)| *network_peer_id == &peer_id)
            .map(|(node_id, _network_peer_id)| *node_id)
            .collect();

        assert_eq!(matching_node_ids.len(), 1);

        matching_node_ids[0]
    }

}

// TODO: break these out into better places
fn make_msg_info(msg_type: &str, view: u64, seq_num: u64) -> PbftMessageInfo {
    let mut info = PbftMessageInfo::new();
    info.set_msg_type(msg_type.into());
    info.set_view(view);
    info.set_seq_num(seq_num);
    info
}

fn make_msg_bytes(info: PbftMessageInfo, block: PbftBlock) -> Vec<u8> {
    let mut msg = PbftMessage::new();
    msg.set_info(info);
    msg.set_block(block);

    msg.write_to_bytes().unwrap_or_else(|err| {
        panic!("Couldn't serialize commit message: {}", err);
    })
}

fn pbft_block_from_block(block: Block) -> PbftBlock {
    let mut pbft_block = PbftBlock::new();
    pbft_block.set_block_id(Vec::<u8>::from(block.block_id));
    pbft_block.set_signer_id(Vec::<u8>::from(block.signer_id));
    pbft_block.set_block_num(block.block_num);
    pbft_block
}
