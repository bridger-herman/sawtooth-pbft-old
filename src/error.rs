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

use std::error::Error;
use std::fmt;

use protobuf::error::ProtobufError;

use protos::pbft_message::PbftBlock;

use message_type::PbftMessageType;

// Errors that might occur in a PbftNode
#[derive(Debug)]
pub enum PbftError {
    SerializationError(ProtobufError),
    MessageExists(PbftMessageType),
    WrongNumMessages(PbftMessageType, usize, usize),
    BlockMismatch(PbftBlock, PbftBlock),
    MessageMismatch(PbftMessageType),
    ViewMismatch(usize, usize),
    NodeNotFound,
    WrongNumBlocks,
    Timeout,
}

impl Error for PbftError {
    fn description(&self) -> &str {
        use self::PbftError::*;
        match self {
            SerializationError(_) => "SerializationError",
            MessageExists(_) => "MessageExists",
            WrongNumMessages(_, _, _) => "WrongNumMessages",
            BlockMismatch(_, _) => "BlockMismatch",
            MessageMismatch(_) => "MessageMismatch",
            ViewMismatch(_, _) => "ViewMismatch",
            NodeNotFound => "NodeNotFound",
            WrongNumBlocks => "WrongNumBlocks",
            Timeout => "Timeout",
        }
    }
}

impl fmt::Display for PbftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: ", self.description())?;
        match self {
            PbftError::SerializationError(pb_err) => pb_err.fmt(f),
            PbftError::MessageExists(t) => write!(
                f,
                "A {:?} message already exists with this sequence number",
                t
            ),
            PbftError::WrongNumMessages(t, exp, got) => write!(
                f,
                "Wrong number of {:?} messages in this sequence (expected {}, got {})",
                t, exp, got
            ),
            PbftError::MessageMismatch(t) => write!(f, "{:?} message mismatch", t),
            PbftError::ViewMismatch(exp, got) => write!(f, "View mismatch: {} != {}", exp, got),
            PbftError::BlockMismatch(exp, got) => {
                write!(f, "Block mismatch: {:?} != {:?}", exp, got)
            }
            PbftError::NodeNotFound => write!(f, "Couldn't find node in the network"),
            PbftError::WrongNumBlocks => write!(f, "Incorrect number of blocks"),
            PbftError::Timeout => write!(f, "Timed out"),
        }
    }
}
