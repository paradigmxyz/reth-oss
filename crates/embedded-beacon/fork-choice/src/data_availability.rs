//! Mandatory retrieval and proof verification for blob and sampling forks.
//! A callback must authenticate the exact candidate data before the event store publishes it.

use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::BeaconBlock;
use std::future::Future;

/// Required actual data availability verification for Deneb, Electra and Fulu imports.
/// Implementations bind retrieved data to the supplied root and complete candidate block,
/// verify all KZG proofs and require every configured sample. Missing retrieval must fail.
/// Blocking proof work belongs on the coordinator's bounded workers. There is no default
/// successful implementation or caller-supplied validity flag.
pub trait DataAvailability: Send + Sync {
    /// Missing data, invalid proofs, retrieval errors or local admission limits.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Verify the exact candidate's configured availability before its strict transition.
    fn verify<'a>(
        &'a self,
        root: Root,
        block: &'a BeaconBlock,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
    /// Retain staged proof data only after this privately minted strict native block receipt.
    /// Stateless callbacks can keep the default; a root or proof-valid flag cannot mint a receipt.
    fn retain_admitted(
        &self,
        _admitted: &crate::compact::AdmittedState<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
