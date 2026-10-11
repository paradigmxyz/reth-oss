//! Portable complete Engine response containers shared by typed verification and RPC dispatch.

use alloy_rpc_types_engine::{
    ForkchoiceUpdated, ForkchoiceUpdatedResponseV2, PayloadStatus, PayloadStatusV2,
};

/// Full engine payload response.
#[derive(Debug)]
pub enum PayloadReply {
    /// Original response fields.
    V1(PayloadStatus),
    /// Extended response fields.
    V2(PayloadStatusV2),
}
/// Full forkchoice response.
#[derive(Debug)]
pub enum ForkchoiceReply {
    /// Original response fields.
    V1(ForkchoiceUpdated),
    /// Extended response fields.
    V2(ForkchoiceUpdatedResponseV2),
}
