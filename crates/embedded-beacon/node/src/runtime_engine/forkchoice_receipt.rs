//! Private receipt for an actual exact VALID canonical Engine acknowledgement.
//!
//! The receipt is minted only after a typed method validates the exact original VALID
//! response. It grants execution acknowledgement for these hashes, never consensus finality.

use super::*;

#[derive(Clone, Debug)]
pub(crate) struct VerifiedForkchoice {
    state: ForkchoiceState,
    binding: Arc<ChainConfig>,
}
impl VerifiedForkchoice {
    pub(crate) fn matches<E>(
        &self,
        engine: &RuntimeEngineAdapter<E>,
        expected: ForkchoiceState,
    ) -> bool
    where
        E: TypedExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        self.state == expected && self.is_bound_to(engine)
    }
    /// Check only this original VALID receipt's adapter, without acknowledging any new target.
    pub(crate) fn is_bound_to<E>(&self, engine: &RuntimeEngineAdapter<E>) -> bool
    where
        E: TypedExecutionEngine<
                Payload = ExecutionData,
                PayloadReply = PayloadReply,
                Forkchoice = ForkchoiceState,
                ForkchoiceReply = ForkchoiceReply,
            > + Clone,
        E::Error: std::error::Error + Send + Sync + 'static,
    {
        !engine.is_closed() && Arc::ptr_eq(&self.binding, &engine.bootstrap_binding())
    }
}
impl<E> RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    /// Execute exactly once through the existing strict original-response contract.
    /// Equal caller configurations or an arbitrary VALID flag cannot construct this receipt.
    pub(crate) async fn forkchoice_updated_receipt(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<VerifiedForkchoice, EngineVerificationError<E::Error>> {
        self.forkchoice_updated(version, state, custody).await?;
        Ok(VerifiedForkchoice { state, binding: self.bootstrap_binding() })
    }
}

#[cfg(all(test, feature = "gossip-validation", feature = "peer-runtime"))]
mod tests;

#[cfg(feature = "gossip-validation")]
impl<E> RuntimeEngineAdapter<E>
where
    E: TypedExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    /// Submit one optimistic download target; only its original exact VALID can mint a receipt.
    /// SYNCING does not acknowledge the target. INVALID and transport errors remain errors.
    pub(crate) async fn optimistic_forkchoice_updated(
        &self,
        version: ForkchoiceVersion,
        state: ForkchoiceState,
        custody: Option<[u8; 16]>,
    ) -> Result<OptimisticForkchoice, EngineVerificationError<E::Error>> {
        let response = self.optimistic_forkchoice_response(version, state, custody).await?;
        match response.status {
            reth_embedded_beacon_fork_choice::store::optimistic::PayloadStatus::Valid => {
                Ok(OptimisticForkchoice::Valid(VerifiedForkchoice {
                    state,
                    binding: self.bootstrap_binding(),
                }))
            }
            reth_embedded_beacon_fork_choice::store::optimistic::PayloadStatus::Syncing => {
                Ok(OptimisticForkchoice::Syncing)
            }
            _ => Err(EngineVerificationError::NotValidated("INVALID")),
        }
    }
}
