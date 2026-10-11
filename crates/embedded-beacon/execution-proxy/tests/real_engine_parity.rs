//! Real execution-engine parity across every supported Engine API version.
//!
//! These tests compare separate databases behind an in-process server call and authenticated
//! HTTP JSON-RPC. They exercise execution, canonicalization, exact errors, and custody propagation.
//! They do not establish beacon state-transition or inclusion-list conformance.

#![cfg(unix)]

use alloy_primitives::{Bytes, B128, B256};
use alloy_rpc_types_engine::{
    ClientCode, ClientVersionV1, ExecutionData, ExecutionPayloadSidecar, ForkchoiceState,
    PraguePayloadFields, INVALID_FORK_CHOICE_STATE_ERROR, INVALID_FORK_CHOICE_STATE_ERROR_MSG,
};
use jsonrpsee::{
    core::{client::ClientT, ClientError},
    rpc_params,
};
use jsonrpsee_types::ErrorObjectOwned;
use reth_chainspec::{EthereumHardfork, ForkCondition};
use reth_e2e_test_utils::{
    test_chain_spec_builder, transaction::TransactionTestContext, E2ETestSetupExt, NodeHelperType,
};
use reth_embedded_beacon_execution_proxy::{
    ExecutionEngine, ExecutionProxy, ForkchoiceReply, ForkchoiceVersion, PayloadReply,
    PayloadVersion, ProxyError, RethTypedEngine,
};
use reth_network_api::NetworkInfo;
use reth_node_ethereum::{EthereumEngineValidator, EthereumNode};
use reth_payload_builder::PayloadStore;
use reth_rpc_engine_api::{EngineApi, EngineApiError, EngineCapabilities};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

type Outcome = Result<Value, ErrorObjectOwned>;

#[tokio::test]
async fn payload_and_forkchoice_parity_across_execution_forks() -> eyre::Result<()> {
    for (fork, version) in [
        (EthereumHardfork::Paris, PayloadVersion::V1),
        (EthereumHardfork::Shanghai, PayloadVersion::V2),
        (EthereumHardfork::Cancun, PayloadVersion::V3),
        (EthereumHardfork::Prague, PayloadVersion::V4),
        (EthereumHardfork::Osaka, PayloadVersion::V4),
        (EthereumHardfork::Amsterdam, PayloadVersion::V5),
        (EthereumHardfork::Bogota, PayloadVersion::V6),
    ] {
        let (mut nodes, wallet) = EthereumNode::test_setup_for(fork)
            .with_num_nodes(2)
            .with_connect_nodes(false)
            .build()
            .await?;
        let rpc_node = nodes.pop().unwrap();
        let mut typed_node = nodes.pop().unwrap();
        let client = rpc_node.auth_server_handle().http_client();
        let proxy = typed_proxy(&typed_node)?;
        let genesis = typed_node.block_hash(0);
        assert_eq!(genesis, rpc_node.block_hash(0));

        for fcu_version in forkchoice_versions() {
            let state = ForkchoiceState::default();
            let typed =
                forkchoice_outcome(proxy.forkchoice_updated(fcu_version, state, None).await);
            let rpc = rpc_forkchoice(&client, fcu_version, state, None).await;
            let expected = Err(ErrorObjectOwned::owned(
                INVALID_FORK_CHOICE_STATE_ERROR,
                INVALID_FORK_CHOICE_STATE_ERROR_MSG,
                None::<()>,
            ));
            assert_eq!(typed, expected, "{fork:?}: {fcu_version:?} zero head");
            assert_eq!(typed, rpc, "{fork:?}: {fcu_version:?} zero-head parity");
        }

        // Execute a funded transfer rather than testing only an empty block.
        let transaction = TransactionTestContext::transfer_tx_bytes(1, wallet.inner).await;
        let transaction_hash = typed_node.rpc.inject_tx(transaction).await?;
        typed_node.wait_for_pooled([transaction_hash]).await?;
        let built = typed_node.new_payload().await?;
        assert_eq!(built.block().body().transactions.len(), 1);
        let mut valid = built.clone().into_execution_data();
        if version == PayloadVersion::V6 {
            valid.sidecar = valid.sidecar.with_inclusion_list(Vec::new());
        }

        if valid.sidecar.prague().is_some() {
            let mut hash_only = valid.clone();
            hash_only.sidecar = ExecutionPayloadSidecar::v4(
                valid.sidecar.cancun().unwrap().clone(),
                PraguePayloadFields::new(valid.sidecar.requests_hash().unwrap()),
            );
            if version == PayloadVersion::V6 {
                hash_only.sidecar = hash_only.sidecar.with_inclusion_list(Vec::new());
            }
            let typed = payload_outcome(proxy.new_payload(version, hash_only.clone()).await);
            let rpc = rpc_payload(&client, version, &hash_only).await;
            let expected = Err(ErrorObjectOwned::from(EngineApiError::UnexpectedRequestsHash));
            assert_eq!(typed, expected, "{fork:?}: hash-only policy");
            assert_eq!(typed, rpc, "{fork:?}: hash-only policy parity");
        }

        // The later method's exact request shape still fails the earlier fork's timestamp gate.
        let future = match version {
            PayloadVersion::V3 => Some((
                PayloadVersion::V4,
                ExecutionData::new(
                    valid.payload.clone(),
                    ExecutionPayloadSidecar::v4(
                        valid.sidecar.cancun().unwrap().clone(),
                        PraguePayloadFields::default(),
                    ),
                ),
            )),
            PayloadVersion::V5 => Some((
                PayloadVersion::V6,
                ExecutionData::new(
                    valid.payload.clone(),
                    valid.sidecar.clone().with_inclusion_list(Vec::new()),
                ),
            )),
            _ => None,
        };
        if let Some((future_version, future_data)) = future {
            let typed =
                payload_outcome(proxy.new_payload(future_version, future_data.clone()).await);
            let rpc = rpc_payload(&client, future_version, &future_data).await;
            assert_eq!(typed, rpc, "{fork:?}: later-method timestamp rejection parity");
            assert_eq!(
                typed.unwrap_err(),
                ErrorObjectOwned::owned(-38005, "Unsupported fork", None::<()>,)
            );
        }
        // Keep the payload self-consistent so execution, rather than hash prevalidation, detects
        // the wrong state root. Both nodes must report the same last valid parent and error.
        let mut wrong_header = built.block().header().clone();
        wrong_header.state_root = B256::ZERO;
        let mut invalid = valid.clone();
        invalid.payload.as_v1_mut().state_root = B256::ZERO;
        invalid.payload.as_v1_mut().block_hash = wrong_header.hash_slow();
        let typed = payload_outcome(proxy.new_payload(version, invalid.clone()).await);
        let rpc = rpc_payload(&client, version, &invalid).await;
        assert_eq!(typed, rpc, "{fork:?}: execution-invalid payload parity");
        let status = typed.unwrap();
        assert_eq!(status["status"], "INVALID");
        assert_eq!(status["latestValidHash"], json!(genesis));
        assert_eq!(typed_node.current_forkchoice_state()?.head_block_hash, genesis);
        assert_eq!(rpc_node.current_forkchoice_state()?.head_block_hash, genesis);

        let mut malformed = valid.clone();
        malformed.payload.as_v1_mut().extra_data = Bytes::from(vec![0xab; 33]);
        let typed = payload_outcome(proxy.new_payload(version, malformed.clone()).await);
        let rpc = rpc_payload(&client, version, &malformed).await;
        assert_eq!(typed, rpc, "{fork:?}: malformed extra-data parity");
        assert_eq!(typed.unwrap()["status"], "INVALID");

        let typed = payload_outcome(proxy.new_payload(version, valid.clone()).await);
        let rpc = rpc_payload(&client, version, &valid).await;
        assert_eq!(typed, rpc, "{fork:?}: valid payload parity");
        let status = typed.unwrap();
        assert_eq!(status["status"], "VALID");
        assert_eq!(status["latestValidHash"], json!(valid.block_hash()));
        // newPayload alone does not choose a canonical head.
        assert_eq!(typed_node.current_forkchoice_state()?.head_block_hash, genesis);
        assert_eq!(rpc_node.current_forkchoice_state()?.head_block_hash, genesis);

        let state = ForkchoiceState {
            head_block_hash: valid.block_hash(),
            safe_block_hash: genesis,
            finalized_block_hash: genesis,
        };
        for fcu_version in forkchoice_versions() {
            let custody = matches!(fcu_version, ForkchoiceVersion::V4 | ForkchoiceVersion::V5)
                .then_some(0x10203u128.to_le_bytes());
            let typed =
                forkchoice_outcome(proxy.forkchoice_updated(fcu_version, state, custody).await);
            let rpc = rpc_forkchoice(&client, fcu_version, state, custody).await;
            assert_eq!(typed, rpc, "{fork:?}: {fcu_version:?} valid-head parity");
            assert_eq!(typed.unwrap()["payloadStatus"]["status"], "VALID");
            if custody.is_some() {
                assert_eq!(typed_node.inner.network.cell_custody().get(), B128::from(0x10203u128));
                assert_eq!(rpc_node.inner.network.cell_custody().get(), B128::from(0x10203u128));
            }
        }
        typed_node.wait_for_head(valid.block_hash()).await?;
        rpc_node.wait_for_head(valid.block_hash()).await?;
        assert_eq!(typed_node.block_hash(1), valid.block_hash());
        assert_eq!(rpc_node.block_hash(1), valid.block_hash());
        assert_eq!(typed_node.current_forkchoice_state()?, rpc_node.current_forkchoice_state()?);

        for fcu_version in forkchoice_versions() {
            let unknown = ForkchoiceState { head_block_hash: B256::repeat_byte(0x55), ..state };
            let typed =
                forkchoice_outcome(proxy.forkchoice_updated(fcu_version, unknown, None).await);
            let rpc = rpc_forkchoice(&client, fcu_version, unknown, None).await;
            assert_eq!(typed, rpc, "{fork:?}: {fcu_version:?} unknown-head parity");
            assert_eq!(typed.unwrap()["payloadStatus"]["status"], "SYNCING");
            let typed =
                forkchoice_outcome(proxy.forkchoice_updated(fcu_version, state, None).await);
            let rpc = rpc_forkchoice(&client, fcu_version, state, None).await;
            assert_eq!(typed, rpc, "{fork:?}: {fcu_version:?} return-to-known-head parity");
            assert_eq!(typed.unwrap()["payloadStatus"]["status"], "VALID");
        }
    }
    Ok(())
}

#[tokio::test]
async fn explicitly_enabled_requests_hash_matches_authenticated_rpc() -> eyre::Result<()> {
    let (mut nodes, _) = EthereumNode::test_setup_for(EthereumHardfork::Prague)
        .with_num_nodes(2)
        .with_connect_nodes(false)
        .with_node_config_modifier(|mut config| {
            config.engine.accept_execution_requests_hash = true;
            config
        })
        .build()
        .await?;
    let rpc_node = nodes.pop().unwrap();
    let mut typed_node = nodes.pop().unwrap();
    let proxy = typed_proxy(&typed_node)?;
    let client = rpc_node.auth_server_handle().http_client();
    let built = typed_node.new_payload().await?;
    // Block-only conversion intentionally contains a requests hash rather than request bytes.
    let data =
        ExecutionData::from_block_unchecked(built.block().hash(), &built.block().clone_block());
    assert!(data.sidecar.prague().unwrap().requests.is_hash());
    let typed = payload_outcome(proxy.new_payload(PayloadVersion::V4, data.clone()).await);
    let rpc = rpc_payload(&client, PayloadVersion::V4, &data).await;
    assert_eq!(typed, rpc);
    assert_eq!(typed.unwrap()["status"], "VALID");
    Ok(())
}

#[tokio::test]
async fn typed_and_rpc_follow_execution_fork_transitions() -> eyre::Result<()> {
    let first_timestamp = 1_710_338_136;
    let mut chain = test_chain_spec_builder().paris_activated();
    for (offset, fork) in [
        (1, EthereumHardfork::Shanghai),
        (2, EthereumHardfork::Cancun),
        (3, EthereumHardfork::Prague),
        (4, EthereumHardfork::Osaka),
        (5, EthereumHardfork::Amsterdam),
        (6, EthereumHardfork::Bogota),
    ] {
        chain = chain.with_fork(fork, ForkCondition::Timestamp(first_timestamp + offset));
    }
    let (mut nodes, _) = EthereumNode::test_setup(2, Arc::new(chain.build()))
        .with_num_nodes(2)
        .with_connect_nodes(false)
        .build()
        .await?;
    let rpc_node = nodes.pop().unwrap();
    let mut typed_node = nodes.pop().unwrap();
    let proxy = typed_proxy(&typed_node)?;
    let client = rpc_node.auth_server_handle().http_client();
    let mut parent = typed_node.block_hash(0);

    for (offset, (version, fcu_version)) in [
        (PayloadVersion::V1, ForkchoiceVersion::V1),
        (PayloadVersion::V2, ForkchoiceVersion::V2),
        (PayloadVersion::V3, ForkchoiceVersion::V3),
        (PayloadVersion::V4, ForkchoiceVersion::V3),
        (PayloadVersion::V4, ForkchoiceVersion::V3),
        (PayloadVersion::V5, ForkchoiceVersion::V4),
        (PayloadVersion::V6, ForkchoiceVersion::V5),
    ]
    .into_iter()
    .enumerate()
    {
        typed_node.set_next_payload_timestamp(first_timestamp + offset as u64)?;
        let built = typed_node.new_payload().await?;
        let mut data = built.into_execution_data();
        if version == PayloadVersion::V6 {
            data.sidecar = data.sidecar.with_inclusion_list(Vec::new());
        }
        assert_eq!(data.parent_hash(), parent);
        assert_eq!(data.block_number(), offset as u64 + 1);
        let typed = payload_outcome(proxy.new_payload(version, data.clone()).await);
        let rpc = rpc_payload(&client, version, &data).await;
        assert_eq!(typed, rpc, "activation {offset}: payload parity");
        assert_eq!(typed.unwrap()["status"], "VALID");
        let state = ForkchoiceState {
            head_block_hash: data.block_hash(),
            safe_block_hash: parent,
            finalized_block_hash: parent,
        };
        let typed = forkchoice_outcome(proxy.forkchoice_updated(fcu_version, state, None).await);
        let rpc = rpc_forkchoice(&client, fcu_version, state, None).await;
        assert_eq!(typed, rpc, "activation {offset}: forkchoice parity");
        assert_eq!(typed.unwrap()["payloadStatus"]["status"], "VALID");
        typed_node.wait_for_head(data.block_hash()).await?;
        rpc_node.wait_for_head(data.block_hash()).await?;
        typed_node.wait_for_pool_head(data.block_hash()).await?;
        assert_eq!(typed_node.block_hash(offset as u64 + 1), data.block_hash());
        assert_eq!(rpc_node.block_hash(offset as u64 + 1), data.block_hash());
        assert_eq!(typed_node.current_forkchoice_state()?, rpc_node.current_forkchoice_state()?);
        parent = data.block_hash();
    }
    Ok(())
}

fn typed_proxy(
    node: &NodeHelperType<EthereumNode>,
) -> eyre::Result<
    ExecutionProxy<
        impl ExecutionEngine<
                Payload = ExecutionData,
                Forkchoice = ForkchoiceState,
                PayloadReply = PayloadReply,
                ForkchoiceReply = ForkchoiceReply,
                Error = ErrorObjectOwned,
            > + use<>,
    >,
> {
    // Reuse the real node's engine handle, provider, validator, payload store, pool and policy.
    // This uses the same public construction path as the default Engine API builder.
    let api = EngineApi::new(
        node.inner.provider.clone(),
        node.inner.config.chain.clone(),
        node.inner.add_ons_handle.consensus_engine_handle().clone(),
        PayloadStore::new(node.inner.payload_builder_handle.clone()),
        node.inner.pool.clone(),
        node.inner.task_executor.clone(),
        ClientVersionV1 {
            code: ClientCode::RH,
            name: "embedded-beacon-parity".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            commit: "00000000".into(),
        },
        EngineCapabilities::default(),
        EthereumEngineValidator::new(node.inner.config.chain.clone()),
        node.inner.config.engine.accept_execution_requests_hash,
        node.inner.network.clone(),
    );
    Ok(ExecutionProxy::new(RethTypedEngine::new(api), Duration::from_secs(30))?)
}

const fn forkchoice_versions() -> [ForkchoiceVersion; 5] {
    [
        ForkchoiceVersion::V1,
        ForkchoiceVersion::V2,
        ForkchoiceVersion::V3,
        ForkchoiceVersion::V4,
        ForkchoiceVersion::V5,
    ]
}

fn payload_outcome(result: Result<PayloadReply, ProxyError<ErrorObjectOwned>>) -> Outcome {
    match result {
        Ok(PayloadReply::V1(status)) => Ok(serde_json::to_value(status).unwrap()),
        Ok(PayloadReply::V2(status)) => Ok(serde_json::to_value(status).unwrap()),
        Err(ProxyError::Engine(error)) => Err(error),
        Err(error) => panic!("proxy admission failure: {error}"),
    }
}

fn forkchoice_outcome(result: Result<ForkchoiceReply, ProxyError<ErrorObjectOwned>>) -> Outcome {
    match result {
        Ok(ForkchoiceReply::V1(status)) => Ok(serde_json::to_value(status).unwrap()),
        Ok(ForkchoiceReply::V2(status)) => Ok(serde_json::to_value(status).unwrap()),
        Err(ProxyError::Engine(error)) => Err(error),
        Err(error) => panic!("proxy admission failure: {error}"),
    }
}

fn rpc_outcome(result: Result<Value, ClientError>) -> Outcome {
    match result {
        Ok(value) => Ok(value),
        Err(ClientError::Call(error)) => Err(error),
        Err(error) => panic!("authenticated RPC transport failure: {error}"),
    }
}

async fn rpc_payload(
    client: &impl ClientT,
    version: PayloadVersion,
    data: &ExecutionData,
) -> Outcome {
    let method = match version {
        PayloadVersion::V1 => "engine_newPayloadV1",
        PayloadVersion::V2 => "engine_newPayloadV2",
        PayloadVersion::V3 => "engine_newPayloadV3",
        PayloadVersion::V4 => "engine_newPayloadV4",
        PayloadVersion::V5 => "engine_newPayloadV5",
        PayloadVersion::V6 => "engine_newPayloadV6",
    };
    let result = match version {
        PayloadVersion::V1 | PayloadVersion::V2 => {
            client.request(method, rpc_params![&data.payload]).await
        }
        PayloadVersion::V3 => {
            let cancun = data.sidecar.cancun().unwrap();
            client
                .request(
                    method,
                    rpc_params![
                        &data.payload,
                        &cancun.versioned_hashes,
                        cancun.parent_beacon_block_root
                    ],
                )
                .await
        }
        PayloadVersion::V4 | PayloadVersion::V5 => {
            let cancun = data.sidecar.cancun().unwrap();
            client
                .request(
                    method,
                    rpc_params![
                        &data.payload,
                        &cancun.versioned_hashes,
                        cancun.parent_beacon_block_root,
                        &data.sidecar.prague().unwrap().requests
                    ],
                )
                .await
        }
        PayloadVersion::V6 => {
            let cancun = data.sidecar.cancun().unwrap();
            client
                .request(
                    method,
                    rpc_params![
                        &data.payload,
                        &cancun.versioned_hashes,
                        cancun.parent_beacon_block_root,
                        &data.sidecar.prague().unwrap().requests,
                        data.sidecar.inclusion_list_transactions().unwrap()
                    ],
                )
                .await
        }
    };
    rpc_outcome(result)
}

async fn rpc_forkchoice(
    client: &impl ClientT,
    version: ForkchoiceVersion,
    state: ForkchoiceState,
    custody: Option<[u8; 16]>,
) -> Outcome {
    let method = match version {
        ForkchoiceVersion::V1 => "engine_forkchoiceUpdatedV1",
        ForkchoiceVersion::V2 => "engine_forkchoiceUpdatedV2",
        ForkchoiceVersion::V3 => "engine_forkchoiceUpdatedV3",
        ForkchoiceVersion::V4 => "engine_forkchoiceUpdatedV4",
        ForkchoiceVersion::V5 => "engine_forkchoiceUpdatedV5",
    };
    let result = if matches!(version, ForkchoiceVersion::V4 | ForkchoiceVersion::V5) {
        client.request(method, rpc_params![state, Value::Null, custody.map(B128::from)]).await
    } else {
        client.request(method, rpc_params![state, Value::Null]).await
    };
    rpc_outcome(result)
}
