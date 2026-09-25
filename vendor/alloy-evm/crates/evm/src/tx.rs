//! Transaction abstractions for EVM execution.
//!
//! This module provides traits and implementations for converting various transaction formats
//! into a unified transaction environment ([`TxEnv`]) that the EVM can execute. The main purpose
//! of these traits is to enable flexible transaction input while maintaining type safety.

use alloc::{boxed::Box, sync::Arc};
use alloy_consensus::{
    crypto::secp256k1, transaction::Recovered, EthereumTxEnvelope, Signed, TxEip1559, TxEip2930,
    TxEip4844, TxEip4844Variant, TxEip7702, TxEip8141, TxLegacy,
};
use alloy_eips::{
    eip2718::WithEncoded,
    eip7702::{RecoveredAuthority, RecoveredAuthorization},
    Typed2718,
};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use revm::{
    context::TxEnv,
    context_interface::{cfg::GasParams, either::Either, transaction::FrameTransaction},
    primitives::hardfork::SpecId,
};

/// Trait marking types that can be converted into a transaction environment.
///
/// This is the primary trait that enables flexible transaction input for the EVM. The EVM's
/// associated type `Evm::Tx` must implement this trait, and the `transact` method accepts
/// any type implementing [`IntoTxEnv<Evm::Tx>`](IntoTxEnv).
///
/// # Example
///
/// ```ignore
/// // Direct TxEnv usage
/// let tx_env = TxEnv { caller: address, gas_limit: 100_000, ... };
/// evm.transact(tx_env)?;
///
/// // Using a recovered transaction
/// let recovered = tx.recover_signer()?;
/// evm.transact(recovered)?;
///
/// // Using a transaction with encoded bytes
/// let with_encoded = WithEncoded::new(recovered, encoded_bytes);
/// evm.transact(with_encoded)?;
/// ```
pub trait IntoTxEnv<TxEnv> {
    /// Converts `self` into [`TxEnv`].
    fn into_tx_env(self) -> TxEnv;

    /// Converts using the active gas schedule. Explicit transaction environments are unchanged.
    fn into_tx_env_with_gas_params(self, _gas_params: &GasParams) -> TxEnv
    where
        Self: Sized,
    {
        self.into_tx_env()
    }
}

impl IntoTxEnv<Self> for TxEnv {
    fn into_tx_env(self) -> Self {
        self
    }
}

/// A helper trait to allow implementing [`IntoTxEnv`] for types that build transaction environment
/// by cloning data.
#[auto_impl::auto_impl(&)]
pub trait ToTxEnv<TxEnv> {
    /// Builds a [`TxEnv`] from `self`.
    fn to_tx_env(&self) -> TxEnv;

    /// Builds a transaction environment using the active gas schedule.
    fn to_tx_env_with_gas_params(&self, _gas_params: &GasParams) -> TxEnv {
        self.to_tx_env()
    }
}

impl<T, TxEnv> IntoTxEnv<TxEnv> for T
where
    T: ToTxEnv<TxEnv>,
{
    fn into_tx_env(self) -> TxEnv {
        self.to_tx_env()
    }

    fn into_tx_env_with_gas_params(self, gas_params: &GasParams) -> TxEnv {
        self.to_tx_env_with_gas_params(gas_params)
    }
}

impl<L, R, TxEnv> ToTxEnv<TxEnv> for Either<L, R>
where
    L: ToTxEnv<TxEnv>,
    R: ToTxEnv<TxEnv>,
{
    fn to_tx_env_with_gas_params(&self, gas_params: &GasParams) -> TxEnv {
        match self {
            Self::Left(l) => l.to_tx_env_with_gas_params(gas_params),
            Self::Right(r) => r.to_tx_env_with_gas_params(gas_params),
        }
    }

    fn to_tx_env(&self) -> TxEnv {
        match self {
            Self::Left(l) => l.to_tx_env(),
            Self::Right(r) => r.to_tx_env(),
        }
    }
}

/// Helper trait for building a transaction environment from a recovered transaction.
///
/// This trait enables the conversion of consensus transaction types (which have been recovered
/// with their sender address) into the EVM's transaction environment. It's automatically used
/// when a [`Recovered<T>`] type is passed to the EVM's `transact` method.
///
/// The expectation is that any recovered consensus transaction can be converted into the
/// transaction type that the EVM operates on (typically [`TxEnv`]).
///
/// # Implementation
///
/// This trait is implemented for all standard Ethereum transaction types ([`TxLegacy`],
/// [`TxEip2930`], [`TxEip1559`], [`TxEip4844`], [`TxEip7702`], [`TxEip8141`]) and
/// transaction envelopes ([`EthereumTxEnvelope`]).
///
/// # Example
///
/// ```ignore
/// // Recover the signer from a transaction
/// let recovered = tx.recover_signer()?;
///
/// // The recovered transaction can now be used with the EVM
/// // This works because Recovered<T> implements IntoTxEnv when T implements FromRecoveredTx
/// evm.transact(recovered)?;
/// ```
pub trait FromRecoveredTx<Tx> {
    /// Builds a [`TxEnv`] from a transaction and a sender address.
    fn from_recovered_tx(tx: &Tx, sender: Address) -> Self;

    /// Builds an environment using the active gas schedule.
    fn from_recovered_tx_with_gas_params(tx: &Tx, sender: Address, _gas_params: &GasParams) -> Self
    where
        Self: Sized,
    {
        Self::from_recovered_tx(tx, sender)
    }
}

impl<TxEnv, T> FromRecoveredTx<&T> for TxEnv
where
    TxEnv: FromRecoveredTx<T>,
{
    fn from_recovered_tx_with_gas_params(tx: &&T, sender: Address, gas_params: &GasParams) -> Self {
        Self::from_recovered_tx_with_gas_params(*tx, sender, gas_params)
    }

    fn from_recovered_tx(tx: &&T, sender: Address) -> Self {
        TxEnv::from_recovered_tx(tx, sender)
    }
}

impl<T, TxEnv: FromRecoveredTx<T>> ToTxEnv<TxEnv> for Recovered<T> {
    fn to_tx_env_with_gas_params(&self, gas_params: &GasParams) -> TxEnv {
        TxEnv::from_recovered_tx_with_gas_params(self.inner(), self.signer(), gas_params)
    }

    fn to_tx_env(&self) -> TxEnv {
        TxEnv::from_recovered_tx(self.inner(), self.signer())
    }
}

impl FromRecoveredTx<TxLegacy> for TxEnv {
    fn from_recovered_tx(tx: &TxLegacy, caller: Address) -> Self {
        let TxLegacy { chain_id, nonce, gas_price, gas_limit, to, value, input } = tx;
        Self {
            tx_type: tx.ty(),
            caller,
            gas_limit: *gas_limit,
            gas_price: *gas_price,
            kind: *to,
            value: *value,
            data: input.clone(),
            nonce: *nonce,
            chain_id: *chain_id,
            ..Default::default()
        }
    }
}

impl FromRecoveredTx<Signed<TxLegacy>> for TxEnv {
    fn from_recovered_tx(tx: &Signed<TxLegacy>, sender: Address) -> Self {
        Self::from_recovered_tx(tx.tx(), sender)
    }
}

impl FromTxWithEncoded<TxLegacy> for TxEnv {
    fn from_encoded_tx(tx: &TxLegacy, sender: Address, _encoded: Bytes) -> Self {
        Self::from_recovered_tx(tx, sender)
    }
}

impl FromRecoveredTx<TxEip2930> for TxEnv {
    fn from_recovered_tx(tx: &TxEip2930, caller: Address) -> Self {
        let TxEip2930 { chain_id, nonce, gas_price, gas_limit, to, value, access_list, input } = tx;
        Self {
            tx_type: tx.ty(),
            caller,
            gas_limit: *gas_limit,
            gas_price: *gas_price,
            kind: *to,
            value: *value,
            data: input.clone(),
            chain_id: Some(*chain_id),
            nonce: *nonce,
            access_list: access_list.clone(),
            ..Default::default()
        }
    }
}

impl FromRecoveredTx<Signed<TxEip2930>> for TxEnv {
    fn from_recovered_tx(tx: &Signed<TxEip2930>, sender: Address) -> Self {
        Self::from_recovered_tx(tx.tx(), sender)
    }
}

impl FromTxWithEncoded<TxEip2930> for TxEnv {
    fn from_encoded_tx(tx: &TxEip2930, sender: Address, _encoded: Bytes) -> Self {
        Self::from_recovered_tx(tx, sender)
    }
}

impl FromRecoveredTx<TxEip1559> for TxEnv {
    fn from_recovered_tx(tx: &TxEip1559, caller: Address) -> Self {
        let TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            to,
            value,
            input,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            access_list,
        } = tx;
        Self {
            tx_type: tx.ty(),
            caller,
            gas_limit: *gas_limit,
            gas_price: *max_fee_per_gas,
            kind: *to,
            value: *value,
            data: input.clone(),
            nonce: *nonce,
            chain_id: Some(*chain_id),
            gas_priority_fee: Some(*max_priority_fee_per_gas),
            access_list: access_list.clone(),
            ..Default::default()
        }
    }
}

impl FromRecoveredTx<Signed<TxEip1559>> for TxEnv {
    fn from_recovered_tx(tx: &Signed<TxEip1559>, sender: Address) -> Self {
        Self::from_recovered_tx(tx.tx(), sender)
    }
}

impl FromTxWithEncoded<TxEip1559> for TxEnv {
    fn from_encoded_tx(tx: &TxEip1559, sender: Address, _encoded: Bytes) -> Self {
        Self::from_recovered_tx(tx, sender)
    }
}

impl FromRecoveredTx<TxEip4844> for TxEnv {
    fn from_recovered_tx(tx: &TxEip4844, caller: Address) -> Self {
        let TxEip4844 {
            chain_id,
            nonce,
            gas_limit,
            to,
            value,
            input,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            access_list,
            blob_versioned_hashes,
            max_fee_per_blob_gas,
        } = tx;
        Self {
            tx_type: tx.ty(),
            caller,
            gas_limit: *gas_limit,
            gas_price: *max_fee_per_gas,
            kind: TxKind::Call(*to),
            value: *value,
            data: input.clone(),
            nonce: *nonce,
            chain_id: Some(*chain_id),
            gas_priority_fee: Some(*max_priority_fee_per_gas),
            access_list: access_list.clone(),
            blob_hashes: blob_versioned_hashes.clone(),
            max_fee_per_blob_gas: *max_fee_per_blob_gas,
            ..Default::default()
        }
    }
}

impl FromRecoveredTx<Signed<TxEip4844>> for TxEnv {
    fn from_recovered_tx(tx: &Signed<TxEip4844>, sender: Address) -> Self {
        Self::from_recovered_tx(tx.tx(), sender)
    }
}

impl FromTxWithEncoded<TxEip4844> for TxEnv {
    fn from_encoded_tx(tx: &TxEip4844, sender: Address, _encoded: Bytes) -> Self {
        Self::from_recovered_tx(tx, sender)
    }
}

impl<T> FromRecoveredTx<TxEip4844Variant<T>> for TxEnv {
    fn from_recovered_tx(tx: &TxEip4844Variant<T>, sender: Address) -> Self {
        Self::from_recovered_tx(tx.tx(), sender)
    }
}

impl<T> FromRecoveredTx<Signed<TxEip4844Variant<T>>> for TxEnv {
    fn from_recovered_tx(tx: &Signed<TxEip4844Variant<T>>, sender: Address) -> Self {
        Self::from_recovered_tx(tx.tx(), sender)
    }
}

impl<T> FromTxWithEncoded<TxEip4844Variant<T>> for TxEnv {
    fn from_encoded_tx(tx: &TxEip4844Variant<T>, sender: Address, _encoded: Bytes) -> Self {
        Self::from_recovered_tx(tx, sender)
    }
}

impl FromRecoveredTx<TxEip7702> for TxEnv {
    fn from_recovered_tx(tx: &TxEip7702, caller: Address) -> Self {
        let TxEip7702 {
            chain_id,
            nonce,
            gas_limit,
            to,
            value,
            input,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            access_list,
            authorization_list,
        } = tx;
        Self {
            tx_type: tx.ty(),
            caller,
            gas_limit: *gas_limit,
            gas_price: *max_fee_per_gas,
            kind: TxKind::Call(*to),
            value: *value,
            data: input.clone(),
            nonce: *nonce,
            chain_id: Some(*chain_id),
            gas_priority_fee: Some(*max_priority_fee_per_gas),
            access_list: access_list.clone(),
            authorization_list: authorization_list
                .iter()
                .map(|auth| {
                    Either::Right(RecoveredAuthorization::new_unchecked(
                        auth.inner().clone(),
                        auth.signature()
                            .ok()
                            .and_then(|signature| {
                                secp256k1::recover_signer(&signature, auth.signature_hash()).ok()
                            })
                            .map_or(RecoveredAuthority::Invalid, RecoveredAuthority::Valid),
                    ))
                })
                .collect(),
            ..Default::default()
        }
    }
}

impl FromRecoveredTx<Signed<TxEip7702>> for TxEnv {
    fn from_recovered_tx(tx: &Signed<TxEip7702>, sender: Address) -> Self {
        Self::from_recovered_tx(tx.tx(), sender)
    }
}

impl FromTxWithEncoded<TxEip7702> for TxEnv {
    fn from_encoded_tx(tx: &TxEip7702, sender: Address, _encoded: Bytes) -> Self {
        Self::from_recovered_tx(tx, sender)
    }
}

impl FromRecoveredTx<TxEip8141> for TxEnv {
    fn from_recovered_tx(tx: &TxEip8141, sender: Address) -> Self {
        Self::from_recovered_tx_with_gas_params(tx, sender, &GasParams::new_spec(SpecId::AMSTERDAM))
    }

    fn from_recovered_tx_with_gas_params(
        tx: &TxEip8141,
        _sender: Address,
        gas_params: &GasParams,
    ) -> Self {
        tx_env_from_eip8141(tx.clone(), gas_params)
    }
}

/// Consumes a frame transaction without cloning its vectors, using the active gas schedule.
pub fn tx_env_from_eip8141(tx: TxEip8141, gas_params: &GasParams) -> TxEnv {
    let signature_hash = tx.signature_hash();
    // EIP-8141 has no outer ECDSA signature. Its consensus sender is an explicit field, so the
    // synthetic signer carried by `Recovered` must not be allowed to override it.
    let frame_transaction = FrameTransaction {
        nonce_keys: tx.nonce_keys,
        nonce_seq: tx.nonce_seq,
        frames: tx.frames,
        signatures: tx.signatures,
        signature_hash,
        max_priority_fee_per_gas: tx.fees.max_priority_fee_per_gas,
        max_fee_per_gas: tx.fees.max_fee_per_gas,
        max_fee_per_blob_gas: tx.fees.max_fee_per_blob_gas,
    };
    let gas_limit =
        frame_transaction.gas_limit_with_params(tx.sender, gas_params).unwrap_or(u64::MAX);

    TxEnv {
        tx_type: TxEip8141::tx_type(),
        caller: tx.sender,
        gas_limit,
        gas_price: tx.fees.max_fee_per_gas.saturating_to(),
        kind: TxKind::Call(tx.sender),
        value: U256::ZERO,
        data: Bytes::new(),
        nonce: tx.nonce_seq,
        chain_id: Some(tx.chain_id),
        gas_priority_fee: Some(tx.fees.max_priority_fee_per_gas.saturating_to()),
        blob_hashes: tx.blob_versioned_hashes,
        max_fee_per_blob_gas: tx.fees.max_fee_per_blob_gas.saturating_to(),
        frame_transaction: Some(Box::new(frame_transaction)),
        ..Default::default()
    }
}

impl FromTxWithEncoded<TxEip8141> for TxEnv {
    fn from_encoded_tx_with_gas_params(
        tx: &TxEip8141,
        sender: Address,
        _encoded: Bytes,
        gas_params: &GasParams,
    ) -> Self {
        Self::from_recovered_tx_with_gas_params(tx, sender, gas_params)
    }

    fn from_encoded_tx(tx: &TxEip8141, sender: Address, _encoded: Bytes) -> Self {
        Self::from_recovered_tx(tx, sender)
    }
}

/// Helper trait to abstract over different [`Recovered<T>`] implementations.
///
/// Implemented for [`Recovered<T>`], `Recovered<&T>`, `&Recovered<T>`, `&Recovered<&T>`
#[auto_impl::auto_impl(&)]
pub trait RecoveredTx<T> {
    /// Returns the transaction.
    fn tx(&self) -> &T;

    /// Returns the signer of the transaction.
    fn signer(&self) -> &Address;
}

impl<T> RecoveredTx<T> for Recovered<&T> {
    fn tx(&self) -> &T {
        self.inner()
    }

    fn signer(&self) -> &Address {
        self.signer_ref()
    }
}

impl<T> RecoveredTx<T> for Recovered<Arc<T>> {
    fn tx(&self) -> &T {
        self.inner().as_ref()
    }

    fn signer(&self) -> &Address {
        self.signer_ref()
    }
}

impl<T> RecoveredTx<T> for Recovered<T> {
    fn tx(&self) -> &T {
        self.inner()
    }

    fn signer(&self) -> &Address {
        self.signer_ref()
    }
}

impl<Tx, T: RecoveredTx<Tx>> RecoveredTx<Tx> for WithEncoded<T> {
    fn tx(&self) -> &Tx {
        self.1.tx()
    }

    fn signer(&self) -> &Address {
        self.1.signer()
    }
}

impl<L, R, Tx> RecoveredTx<Tx> for Either<L, R>
where
    L: RecoveredTx<Tx>,
    R: RecoveredTx<Tx>,
{
    fn tx(&self) -> &Tx {
        match self {
            Self::Left(l) => l.tx(),
            Self::Right(r) => r.tx(),
        }
    }

    fn signer(&self) -> &Address {
        match self {
            Self::Left(l) => l.signer(),
            Self::Right(r) => r.signer(),
        }
    }
}

impl<Tx, T: RecoveredTx<Tx>> RecoveredTx<Tx> for Arc<T> {
    fn tx(&self) -> &Tx {
        (**self).tx()
    }

    fn signer(&self) -> &Address {
        (**self).signer()
    }
}

/// Helper trait for building a transaction environment from a transaction with its encoded form.
///
/// This trait enables the conversion of consensus transaction types along with their EIP-2718
/// encoded bytes into the EVM's transaction environment. It's automatically used when a
/// [`WithEncoded<Recovered<T>>`](WithEncoded) type is passed to the EVM's `transact` method.
///
/// The main purpose of this trait is to allow preserving the original encoded transaction data
/// alongside the parsed transaction, which can be useful for:
/// - Signature verification
/// - Transaction hash computation
/// - Re-encoding for network propagation
/// - Optimism transaction handling (which requires encoded data, for Data availability costs).
///
/// # Implementation
///
/// Most implementations simply delegate to [`FromRecoveredTx`], ignoring the encoded bytes.
/// However, specialized implementations (like Optimism's `OpTransaction`) may use the encoded
/// data for additional functionality.
///
/// # Example
///
/// ```ignore
/// // Create a transaction with its encoded form
/// let encoded_bytes = tx.encoded_2718();
/// let recovered = tx.recover_signer()?;
/// let with_encoded = WithEncoded::new(recovered, encoded_bytes);
///
/// // The transaction with encoded data can be used with the EVM
/// evm.transact(with_encoded)?;
/// ```
pub trait FromTxWithEncoded<Tx> {
    /// Builds a [`TxEnv`] from a transaction, its sender, and encoded transaction bytes.
    fn from_encoded_tx(tx: &Tx, sender: Address, encoded: Bytes) -> Self;

    /// Builds an encoded transaction's environment using the active gas schedule.
    fn from_encoded_tx_with_gas_params(
        tx: &Tx,
        sender: Address,
        encoded: Bytes,
        _gas_params: &GasParams,
    ) -> Self
    where
        Self: Sized,
    {
        Self::from_encoded_tx(tx, sender, encoded)
    }
}

impl<TxEnv, T> FromTxWithEncoded<&T> for TxEnv
where
    TxEnv: FromTxWithEncoded<T>,
{
    fn from_encoded_tx_with_gas_params(
        tx: &&T,
        sender: Address,
        encoded: Bytes,
        gas_params: &GasParams,
    ) -> Self {
        Self::from_encoded_tx_with_gas_params(*tx, sender, encoded, gas_params)
    }

    fn from_encoded_tx(tx: &&T, sender: Address, encoded: Bytes) -> Self {
        TxEnv::from_encoded_tx(tx, sender, encoded)
    }
}

impl<T, TxEnv: FromTxWithEncoded<T>> ToTxEnv<TxEnv> for WithEncoded<Recovered<T>> {
    fn to_tx_env_with_gas_params(&self, gas_params: &GasParams) -> TxEnv {
        let recovered = &self.1;
        TxEnv::from_encoded_tx_with_gas_params(
            recovered.inner(),
            recovered.signer(),
            self.encoded_bytes().clone(),
            gas_params,
        )
    }

    fn to_tx_env(&self) -> TxEnv {
        let recovered = &self.1;
        TxEnv::from_encoded_tx(recovered.inner(), recovered.signer(), self.encoded_bytes().clone())
    }
}

impl<T, TxEnv: FromTxWithEncoded<T>> ToTxEnv<TxEnv> for WithEncoded<&Recovered<T>> {
    fn to_tx_env_with_gas_params(&self, gas_params: &GasParams) -> TxEnv {
        TxEnv::from_encoded_tx_with_gas_params(
            self.value(),
            *self.value().signer(),
            self.encoded_bytes().clone(),
            gas_params,
        )
    }

    fn to_tx_env(&self) -> TxEnv {
        TxEnv::from_encoded_tx(self.value(), *self.value().signer(), self.encoded_bytes().clone())
    }
}

impl<Eip4844: AsRef<TxEip4844>> FromTxWithEncoded<EthereumTxEnvelope<Eip4844>> for TxEnv {
    fn from_encoded_tx_with_gas_params(
        tx: &EthereumTxEnvelope<Eip4844>,
        caller: Address,
        encoded: Bytes,
        gas_params: &GasParams,
    ) -> Self {
        if let EthereumTxEnvelope::Eip8141(tx) = tx {
            return Self::from_encoded_tx_with_gas_params(tx.inner(), caller, encoded, gas_params);
        }
        Self::from_encoded_tx(tx, caller, encoded)
    }

    fn from_encoded_tx(tx: &EthereumTxEnvelope<Eip4844>, caller: Address, encoded: Bytes) -> Self {
        match tx {
            EthereumTxEnvelope::Legacy(tx) => Self::from_encoded_tx(tx.tx(), caller, encoded),
            EthereumTxEnvelope::Eip1559(tx) => Self::from_encoded_tx(tx.tx(), caller, encoded),
            EthereumTxEnvelope::Eip2930(tx) => Self::from_encoded_tx(tx.tx(), caller, encoded),
            EthereumTxEnvelope::Eip4844(tx) => {
                Self::from_encoded_tx(tx.tx().as_ref(), caller, encoded)
            }
            EthereumTxEnvelope::Eip7702(tx) => Self::from_encoded_tx(tx.tx(), caller, encoded),
            EthereumTxEnvelope::Eip8141(tx) => Self::from_encoded_tx(tx.inner(), caller, encoded),
        }
    }
}

impl<Eip4844: AsRef<TxEip4844>> FromRecoveredTx<EthereumTxEnvelope<Eip4844>> for TxEnv {
    fn from_recovered_tx_with_gas_params(
        tx: &EthereumTxEnvelope<Eip4844>,
        sender: Address,
        gas_params: &GasParams,
    ) -> Self {
        if let EthereumTxEnvelope::Eip8141(tx) = tx {
            return Self::from_recovered_tx_with_gas_params(tx.inner(), sender, gas_params);
        }
        Self::from_recovered_tx(tx, sender)
    }

    fn from_recovered_tx(tx: &EthereumTxEnvelope<Eip4844>, sender: Address) -> Self {
        match tx {
            EthereumTxEnvelope::Legacy(tx) => Self::from_recovered_tx(tx.tx(), sender),
            EthereumTxEnvelope::Eip1559(tx) => Self::from_recovered_tx(tx.tx(), sender),
            EthereumTxEnvelope::Eip2930(tx) => Self::from_recovered_tx(tx.tx(), sender),
            EthereumTxEnvelope::Eip4844(tx) => Self::from_recovered_tx(tx.tx().as_ref(), sender),
            EthereumTxEnvelope::Eip7702(tx) => Self::from_recovered_tx(tx.tx(), sender),
            EthereumTxEnvelope::Eip8141(tx) => Self::from_recovered_tx(tx.inner(), sender),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use alloy_eips::eip8141::{
        Frame, FrameLimits, FrameMode, FrameSignature, SignatureScheme, TransactionFees,
    };
    use alloy_primitives::{address, b256, Sealable, U256};

    struct MyTxEnv;
    struct MyTransaction;

    impl IntoTxEnv<Self> for MyTxEnv {
        fn into_tx_env(self) -> Self {
            self
        }
    }

    impl FromRecoveredTx<MyTransaction> for MyTxEnv {
        fn from_recovered_tx(_tx: &MyTransaction, _sender: Address) -> Self {
            Self
        }
    }

    impl FromTxWithEncoded<MyTransaction> for MyTxEnv {
        fn from_encoded_tx(_tx: &MyTransaction, _sender: Address, _encoded: Bytes) -> Self {
            Self
        }
    }

    const fn assert_env<T: IntoTxEnv<MyTxEnv>>() {}
    const fn assert_recoverable<T: RecoveredTx<MyTransaction>>() {}

    #[test]
    const fn test_into_tx_env() {
        assert_env::<MyTxEnv>();
        assert_env::<&Recovered<MyTransaction>>();
        assert_env::<&Recovered<&MyTransaction>>();
    }

    #[test]
    const fn test_into_encoded_tx_env() {
        assert_env::<WithEncoded<Recovered<MyTransaction>>>();
        assert_env::<&WithEncoded<Recovered<MyTransaction>>>();

        assert_recoverable::<Recovered<MyTransaction>>();
        assert_recoverable::<WithEncoded<Recovered<MyTransaction>>>();
    }

    #[test]
    fn converts_eip8141_into_canonical_tx_env() {
        let sender = address!("0000000000000000000000000000000000000001");
        let blob_hash = b256!("0100000000000000000000000000000000000000000000000000000000000001");
        let tx = TxEip8141 {
            chain_id: 1,
            nonce_keys: vec![U256::from(9)],
            nonce_seq: 7,
            sender,
            frames: vec![Frame {
                mode: FrameMode::Sender,
                limits: FrameLimits { execution: 100, state: 0 },
                value: U256::from(3),
                ..Default::default()
            }],
            signatures: vec![FrameSignature {
                scheme: SignatureScheme::Arbitrary,
                ..Default::default()
            }],
            fees: TransactionFees {
                max_priority_fee_per_gas: U256::from(2),
                max_fee_per_gas: U256::from(10),
                max_fee_per_blob_gas: U256::from(4),
            },
            blob_versioned_hashes: vec![blob_hash],
        };
        let signature_hash = tx.signature_hash();

        let envelope = EthereumTxEnvelope::<TxEip4844>::Eip8141(tx.clone().seal_slow());
        let env = TxEnv::from_recovered_tx(&envelope, Address::ZERO);
        let frame_transaction = env.frame_transaction.as_ref().expect("frame transaction");

        assert_eq!(env.tx_type, tx.ty());
        assert_eq!(env.caller, sender);
        assert_eq!(env.kind, TxKind::Call(sender));
        assert_eq!(env.nonce, tx.nonce_seq);
        assert_eq!(frame_transaction.nonce_keys, tx.nonce_keys);
        assert_eq!(frame_transaction.nonce_seq, tx.nonce_seq);
        assert_eq!(env.chain_id, Some(tx.chain_id));
        assert_eq!(env.gas_price, tx.fees.max_fee_per_gas.saturating_to());
        assert_eq!(env.gas_priority_fee, Some(tx.fees.max_priority_fee_per_gas.saturating_to()));
        assert_eq!(env.blob_hashes, tx.blob_versioned_hashes);
        assert_eq!(env.max_fee_per_blob_gas, tx.fees.max_fee_per_blob_gas.saturating_to());
        assert_eq!(env.gas_limit, frame_transaction.gas_limit(sender).unwrap());
        assert_eq!(frame_transaction.signature_hash, signature_hash);
        assert_eq!(frame_transaction.max_priority_fee_per_gas, tx.fees.max_priority_fee_per_gas);
        assert_eq!(frame_transaction.max_fee_per_gas, tx.fees.max_fee_per_gas);
        assert_eq!(frame_transaction.max_fee_per_blob_gas, tx.fees.max_fee_per_blob_gas);
        assert_eq!(frame_transaction.frames, tx.frames);
        assert_eq!(frame_transaction.signatures, tx.signatures);
    }
}
