use super::{
    headers::{empty_capella, empty_header, to_capella, to_deneb},
    types::branch_depths,
    Bootstrap, Header, LightClientError, LightClientParameters, LightClientStore, Update,
};
use crate::ssz::{auxiliary, deneb, Preset, Ssz, Vector};
use reth_embedded_beacon_primitives::{hash_pair, Fork, Root};

fn normalize(branch: &[Root], depth: usize) -> Result<Vec<Root>, LightClientError> {
    if branch.len() > depth {
        return Err(LightClientError::Invalid("cannot shrink a Merkle branch"))
    }
    let mut result = vec![[0; 32]; depth - branch.len()];
    result.extend_from_slice(branch);
    Ok(result)
}
fn execution_block_hash_proof(
    execution: &deneb::ExecutionPayloadHeader,
    deneb: bool,
    preset: Preset,
) -> Result<Vec<Root>, LightClientError> {
    let mut roots = vec![
        execution.parent_hash.hash_tree_root(preset)?,
        execution.fee_recipient.hash_tree_root(preset)?,
        execution.state_root.hash_tree_root(preset)?,
        execution.receipts_root.hash_tree_root(preset)?,
        execution.logs_bloom.hash_tree_root(preset)?,
        execution.prev_randao.hash_tree_root(preset)?,
        execution.block_number.hash_tree_root(preset)?,
        execution.gas_limit.hash_tree_root(preset)?,
        execution.gas_used.hash_tree_root(preset)?,
        execution.timestamp.hash_tree_root(preset)?,
        execution.extra_data.hash_tree_root(preset)?,
        execution.base_fee_per_gas.hash_tree_root(preset)?,
        execution.block_hash.hash_tree_root(preset)?,
        execution.transactions_root.hash_tree_root(preset)?,
        execution.withdrawals_root.hash_tree_root(preset)?,
    ];
    if deneb {
        roots.extend([
            execution.blob_gas_used.hash_tree_root(preset)?,
            execution.excess_blob_gas.hash_tree_root(preset)?,
        ])
    }
    roots.resize(roots.len().next_power_of_two(), [0; 32]);
    let mut index = 12;
    let mut proof = Vec::new();
    while roots.len() > 1 {
        proof.push(roots[index ^ 1]);
        roots = roots.chunks_exact(2).map(|pair| hash_pair(pair[0], pair[1])).collect();
        index /= 2;
    }
    Ok(proof)
}
/// Upgrade complete header fields and construct the required execution block-hash proof.
/// The upgrade is structural; it grants no proof or signature validation authority.
pub fn upgrade_header(
    header: &Header,
    target: Fork,
    params: &LightClientParameters,
) -> Result<Header, LightClientError> {
    params.validate()?;
    branch_depths(target)?;
    header.hash_tree_root(params.preset())?;
    if header.matches_schema(target) {
        return Ok(header.clone())
    }
    let mut current = header.clone();
    if let Header::Altair(pre) = &current {
        if target < Fork::Capella {
            return Ok(current)
        }
        current = Header::Capella(Box::new(auxiliary::capella::LightClientHeader {
            beacon: pre.beacon,
            execution: empty_capella(),
            execution_branch: Vector::from_vec(vec![[0; 32]; 4]),
        }));
    }
    if let Header::Capella(pre) = &current {
        if target < Fork::Capella {
            return Err(LightClientError::Unsupported(target))
        }
        if target == Fork::Capella {
            return Ok(current)
        }
        current = Header::Deneb(Box::new(auxiliary::deneb::LightClientHeader {
            beacon: pre.beacon,
            execution: to_deneb(&pre.execution),
            execution_branch: pre.execution_branch.clone(),
        }));
    }
    if let Header::Deneb(pre) = &current {
        if target < Fork::Deneb {
            return Err(LightClientError::Unsupported(target))
        }
        if target < Fork::Gloas {
            return Ok(current)
        }
        if current == empty_header(Fork::Electra, params.preset())? {
            return empty_header(target, params.preset())
        }
        let epoch = pre.beacon.slot / params.slots_per_epoch();
        let (block_hash, proof) = if params.epoch(Fork::Deneb) != u64::MAX &&
            epoch >= params.epoch(Fork::Deneb)
        {
            let mut proof = execution_block_hash_proof(&pre.execution, true, params.preset())?;
            proof.extend_from_slice(&pre.execution_branch.values);
            (pre.execution.block_hash, normalize(&proof, 11)?)
        } else if params.epoch(Fork::Capella) != u64::MAX && epoch >= params.epoch(Fork::Capella) {
            let projected = to_deneb(&to_capella(&pre.execution));
            let mut proof = execution_block_hash_proof(&projected, false, params.preset())?;
            proof.extend_from_slice(&pre.execution_branch.values);
            (pre.execution.block_hash, normalize(&proof, 11)?)
        } else {
            ([0; 32], vec![[0; 32]; 11])
        };
        current = Header::Gloas(Box::new(auxiliary::gloas::LightClientHeader {
            beacon: pre.beacon,
            execution_block_hash: block_hash,
            execution_branch: Vector::from_vec(proof),
        }));
    }
    if !current.matches_schema(target) {
        return Err(LightClientError::Unsupported(target))
    }
    Ok(current)
}
/// Normalize a bootstrap to a later store schema with exact leading-zero branch padding.
pub fn upgrade_bootstrap(
    bootstrap: &Bootstrap,
    target: Fork,
    params: &LightClientParameters,
) -> Result<Bootstrap, LightClientError> {
    if target < bootstrap.schema {
        return Err(LightClientError::Unsupported(target))
    }
    let (depth, _) = branch_depths(target)?;
    Ok(Bootstrap {
        schema: target,
        header: upgrade_header(&bootstrap.header, target, params)?,
        current_sync_committee: bootstrap.current_sync_committee.clone(),
        current_sync_committee_branch: normalize(&bootstrap.current_sync_committee_branch, depth)?,
    })
}
/// Normalize an update to a later schema while retaining every signature and proof field.
pub fn upgrade_update(
    update: &Update,
    target: Fork,
    params: &LightClientParameters,
) -> Result<Update, LightClientError> {
    if target < update.schema {
        return Err(LightClientError::Unsupported(target))
    }
    let (committee_depth, finality_depth) = branch_depths(target)?;
    Ok(Update {
        schema: target,
        attested_header: upgrade_header(&update.attested_header, target, params)?,
        next_sync_committee: update.next_sync_committee.clone(),
        next_sync_committee_branch: normalize(&update.next_sync_committee_branch, committee_depth)?,
        finalized_header: upgrade_header(&update.finalized_header, target, params)?,
        finality_branch: normalize(&update.finality_branch, finality_depth)?,
        sync_aggregate: update.sync_aggregate.clone(),
        signature_slot: update.signature_slot,
    })
}
/// Atomically upgrade a verified store; early schema upgrades are explicitly permitted.
pub fn upgrade_store(
    store: &mut LightClientStore,
    target: Fork,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    if &store.parameters != params {
        return Err(LightClientError::Invalid(
            "store parameters differ from authenticated bootstrap",
        ))
    }
    if target < store.schema {
        return Err(LightClientError::Unsupported(target))
    }
    let mut candidate = store.clone();
    candidate.finalized_header = upgrade_header(&store.finalized_header, target, params)?;
    candidate.optimistic_header = upgrade_header(&store.optimistic_header, target, params)?;
    candidate.best_valid_update = store
        .best_valid_update
        .as_ref()
        .map(|update| upgrade_update(update, target, params))
        .transpose()?;
    candidate.schema = target;
    *store = candidate;
    Ok(())
}
