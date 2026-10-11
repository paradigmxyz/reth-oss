use super::{
    types::{branch_depths, empty_committee},
    Bootstrap, FinalityUpdate, LightClientError, LightClientParameters, LightClientStore,
    OptimisticUpdate, Update,
};
use reth_embedded_beacon_crypto::{compute_domain, compute_signing_root, fast_aggregate_verify};
use reth_embedded_beacon_primitives::{hash_pair, Fork, Root};

/// Check a normalized proof, including every required leading zero and exact path depth.
pub fn is_valid_normalized_merkle_branch(
    leaf: Root,
    branch: &[Root],
    gindex: u64,
    root: Root,
) -> bool {
    if gindex == 0 {
        return false
    }
    let depth = (63 - gindex.leading_zeros()) as usize;
    if branch.len() < depth {
        return false
    }
    let extra = branch.len() - depth;
    if branch[..extra].iter().any(|node| *node != [0; 32]) {
        return false
    }
    let mut value = leaf;
    for (level, sibling) in branch[extra..].iter().enumerate() {
        value = if ((gindex >> level) & 1) == 1 {
            hash_pair(*sibling, value)
        } else {
            hash_pair(value, *sibling)
        };
    }
    value == root
}
/// Compute the period using the actual trusted epoch clock and preset committee period.
pub fn compute_sync_committee_period_at_slot(
    slot: u64,
    params: &LightClientParameters,
) -> Result<u64, LightClientError> {
    params.validate()?;
    Ok(slot / params.slots_per_epoch() / params.epochs_per_sync_committee_period())
}
const fn period(slot: u64, params: &LightClientParameters) -> u64 {
    slot / params.slots_per_epoch() / params.epochs_per_sync_committee_period()
}
pub(super) fn gindices(slot: u64, params: &LightClientParameters) -> (u64, u64, u64) {
    let epoch = slot / params.slots_per_epoch();
    if params.epoch(Fork::Gloas) != u64::MAX && epoch >= params.epoch(Fork::Gloas) {
        (2945, 2946, 735)
    } else if params.epoch(Fork::Electra) != u64::MAX && epoch >= params.epoch(Fork::Electra) {
        (86, 87, 169)
    } else {
        (54, 55, 105)
    }
}
/// A nonzero next-committee branch marks a committee update.
pub fn is_sync_committee_update(update: &Update) -> bool {
    update.next_sync_committee_branch.iter().any(|node| *node != [0; 32])
}
/// A nonzero finality branch marks a finalized checkpoint proof.
pub fn is_finality_update(update: &Update) -> bool {
    update.finality_branch.iter().any(|node| *node != [0; 32])
}
/// Whether the store has learned an authenticated next committee.
pub fn is_next_sync_committee_known(store: &LightClientStore) -> bool {
    store.next_sync_committee.aggregate_pubkey != [0; 48] ||
        store.next_sync_committee.pubkeys.values.iter().any(|key| *key != [0; 48])
}
/// Half of the greater maximum participation count from the last two periods.
pub fn get_safety_threshold(store: &LightClientStore) -> u64 {
    store.previous_max_active_participants.max(store.current_max_active_participants) / 2
}
fn participants(update: &Update) -> u64 {
    update
        .sync_aggregate
        .sync_committee_bits
        .bits
        .as_ssz()
        .iter()
        .map(|byte| u64::from(byte.count_ones()))
        .sum()
}
fn validate_shape(update: &Update, params: &LightClientParameters) -> Result<(), LightClientError> {
    let (committee_depth, finality_depth) = branch_depths(update.schema)?;
    if !update.attested_header.matches_schema(update.schema) ||
        !update.finalized_header.matches_schema(update.schema) ||
        update.next_sync_committee_branch.len() != committee_depth ||
        update.finality_branch.len() != finality_depth
    {
        return Err(LightClientError::Invalid("update schema or branch length mismatch"))
    }
    update.attested_header.hash_tree_root(params.preset())?;
    update.finalized_header.hash_tree_root(params.preset())?;
    update.next_sync_committee.hash_tree_root(params.preset())?;
    update.sync_aggregate.hash_tree_root(params.preset())?;
    Ok(())
}
/// Compare valid updates by the exact supermajority, committee, finality and age priorities.
pub fn is_better_update(
    new: &Update,
    old: &Update,
    params: &LightClientParameters,
) -> Result<bool, LightClientError> {
    params.validate()?;
    validate_shape(new, params)?;
    validate_shape(old, params)?;
    let new_count = participants(new);
    let old_count = participants(old);
    let size = new.sync_aggregate.sync_committee_bits.bits.len() as u64;
    let new_super = new_count * 3 >= size * 2;
    let old_super = old_count * 3 >= size * 2;
    if new_super != old_super {
        return Ok(new_super)
    }
    if !new_super && new_count != old_count {
        return Ok(new_count > old_count)
    }
    let new_relevant = is_sync_committee_update(new) &&
        period(new.attested_header.beacon().slot, params) == period(new.signature_slot, params);
    let old_relevant = is_sync_committee_update(old) &&
        period(old.attested_header.beacon().slot, params) == period(old.signature_slot, params);
    if new_relevant != old_relevant {
        return Ok(new_relevant)
    }
    let new_finality = is_finality_update(new);
    let old_finality = is_finality_update(old);
    if new_finality != old_finality {
        return Ok(new_finality)
    }
    if new_finality {
        let new_committee_finality = period(new.finalized_header.beacon().slot, params) ==
            period(new.attested_header.beacon().slot, params);
        let old_committee_finality = period(old.finalized_header.beacon().slot, params) ==
            period(old.attested_header.beacon().slot, params);
        if new_committee_finality != old_committee_finality {
            return Ok(new_committee_finality)
        }
    }
    if new_count != old_count {
        return Ok(new_count > old_count)
    }
    if new.attested_header.beacon().slot != old.attested_header.beacon().slot {
        return Ok(new.attested_header.beacon().slot < old.attested_header.beacon().slot)
    }
    Ok(new.signature_slot < old.signature_slot)
}
/// Authenticate the requested block root and the included current sync committee.
pub fn initialize_light_client_store(
    trusted_block_root: Root,
    bootstrap: &Bootstrap,
    params: &LightClientParameters,
) -> Result<LightClientStore, LightClientError> {
    params.validate()?;
    let (committee_depth, _) = branch_depths(bootstrap.schema)?;
    if !bootstrap.header.matches_schema(bootstrap.schema) ||
        bootstrap.current_sync_committee_branch.len() != committee_depth
    {
        return Err(LightClientError::Invalid("bootstrap schema or branch length mismatch"))
    }
    if !super::is_valid_light_client_header(&bootstrap.header, params)? ||
        (*bootstrap.header.beacon()).hash_tree_root() != trusted_block_root
    {
        return Err(LightClientError::Invalid("bootstrap header differs from trusted block root"))
    }
    if !is_valid_normalized_merkle_branch(
        bootstrap.current_sync_committee.hash_tree_root(params.preset())?,
        &bootstrap.current_sync_committee_branch,
        gindices(bootstrap.header.beacon().slot, params).0,
        bootstrap.header.beacon().state_root,
    ) {
        return Err(LightClientError::Invalid("bootstrap current committee proof failed"))
    }
    Ok(LightClientStore {
        schema: bootstrap.schema,
        parameters: params.clone(),
        finalized_header: bootstrap.header.clone(),
        current_sync_committee: bootstrap.current_sync_committee.clone(),
        next_sync_committee: empty_committee(params.preset()),
        best_valid_update: None,
        optimistic_header: bootstrap.header.clone(),
        previous_max_active_participants: 0,
        current_max_active_participants: 0,
    })
}
/// Verify full timing, period, Merkle-proof and real BLS rules without mutating the store.
pub fn validate_light_client_update(
    store: &LightClientStore,
    update: &Update,
    current_slot: u64,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    params.validate()?;
    validate_shape(update, params)?;
    if &store.parameters != params {
        return Err(LightClientError::Invalid(
            "store parameters differ from authenticated bootstrap",
        ))
    }
    if store.schema != update.schema {
        return Err(LightClientError::Invalid("update must be upgraded to store schema"))
    }
    if participants(update) == 0 {
        return Err(LightClientError::Invalid("sync committee has no participants"))
    }
    let attested_slot = update.attested_header.beacon().slot;
    let finalized_slot = update.finalized_header.beacon().slot;
    if !super::is_valid_light_client_header(&update.attested_header, params)? ||
        current_slot < update.signature_slot ||
        update.signature_slot <= attested_slot ||
        attested_slot < finalized_slot
    {
        return Err(LightClientError::Invalid("header validity or update slot order failed"))
    }
    let store_period = period(store.finalized_header.beacon().slot, params);
    let signature_period = period(update.signature_slot, params);
    let known = is_next_sync_committee_known(store);
    if signature_period != store_period &&
        (!known || store_period.checked_add(1) != Some(signature_period))
    {
        return Err(LightClientError::Invalid("update skips an authenticated committee period"))
    }
    let attested_period = period(attested_slot, params);
    let supplies_next =
        !known && is_sync_committee_update(update) && attested_period == store_period;
    if attested_slot <= store.finalized_header.beacon().slot && !supplies_next {
        return Err(LightClientError::Invalid("update is not relevant"))
    }
    let empty_header = super::headers::empty_header(update.schema, params.preset())?;
    if is_finality_update(update) {
        let finalized_root = if finalized_slot == 0 {
            if update.finalized_header != empty_header {
                return Err(LightClientError::Invalid("genesis finalized header must be empty"))
            }
            [0; 32]
        } else {
            if !super::is_valid_light_client_header(&update.finalized_header, params)? {
                return Err(LightClientError::Invalid("finalized execution header proof failed"))
            }
            (*update.finalized_header.beacon()).hash_tree_root()
        };
        if !is_valid_normalized_merkle_branch(
            finalized_root,
            &update.finality_branch,
            gindices(attested_slot, params).2,
            update.attested_header.beacon().state_root,
        ) {
            return Err(LightClientError::Invalid("finality checkpoint proof failed"))
        }
    } else {
        if update.finalized_header != empty_header {
            return Err(LightClientError::Invalid("absent finality proof has a nonempty header"))
        }
    }
    if is_sync_committee_update(update) {
        if attested_period == store_period &&
            known &&
            update.next_sync_committee != store.next_sync_committee
        {
            return Err(LightClientError::Invalid(
                "proved next committee differs from known committee",
            ))
        }
        if !is_valid_normalized_merkle_branch(
            update.next_sync_committee.hash_tree_root(params.preset())?,
            &update.next_sync_committee_branch,
            gindices(attested_slot, params).1,
            update.attested_header.beacon().state_root,
        ) {
            return Err(LightClientError::Invalid("next committee proof failed"))
        }
    } else {
        if update.next_sync_committee != empty_committee(params.preset()) {
            return Err(LightClientError::Invalid("absent committee proof has a nonempty committee"))
        }
    }
    let committee = if signature_period == store_period {
        &store.current_sync_committee
    } else {
        &store.next_sync_committee
    };
    committee.hash_tree_root(params.preset())?;
    let keys: Vec<_> = committee
        .pubkeys
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, key)| {
            (update.sync_aggregate.sync_committee_bits.bits.get(index) == Some(true))
                .then_some(*key)
        })
        .collect();
    let domain = compute_domain(
        [7, 0, 0, 0],
        params.version_at_slot(update.signature_slot.saturating_sub(1))?,
        params.genesis_validators_root,
    );
    fast_aggregate_verify(
        &keys,
        &compute_signing_root((*update.attested_header.beacon()).hash_tree_root(), domain),
        &update.sync_aggregate.sync_committee_signature,
    )?;
    Ok(())
}
fn apply_update(
    store: &mut LightClientStore,
    update: &Update,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    let store_period = period(store.finalized_header.beacon().slot, params);
    let finalized_period = period(update.finalized_header.beacon().slot, params);
    if !is_next_sync_committee_known(store) {
        if finalized_period != store_period {
            return Err(LightClientError::Invalid(
                "unknown next committee cannot cross finalized period",
            ))
        }
        store.next_sync_committee = update.next_sync_committee.clone();
    } else if store_period.checked_add(1) == Some(finalized_period) {
        store.current_sync_committee = store.next_sync_committee.clone();
        store.next_sync_committee = update.next_sync_committee.clone();
        store.previous_max_active_participants = store.current_max_active_participants;
        store.current_max_active_participants = 0;
    }
    if update.finalized_header.beacon().slot > store.finalized_header.beacon().slot {
        store.finalized_header = update.finalized_header.clone();
        if store.finalized_header.beacon().slot > store.optimistic_header.beacon().slot {
            store.optimistic_header = store.finalized_header.clone()
        }
    }
    Ok(())
}
/// Atomically process a fully verified update and its exact safety-threshold/finality rules.
pub fn process_light_client_update(
    store: &mut LightClientStore,
    update: &Update,
    current_slot: u64,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    validate_light_client_update(store, update, current_slot, params)?;
    let mut candidate = store.clone();
    if candidate
        .best_valid_update
        .as_ref()
        .map_or(Ok(true), |old| is_better_update(update, old, params))?
    {
        candidate.best_valid_update = Some(update.clone());
    }
    let count = participants(update);
    candidate.current_max_active_participants =
        candidate.current_max_active_participants.max(count);
    if count > get_safety_threshold(&candidate) &&
        update.attested_header.beacon().slot > candidate.optimistic_header.beacon().slot
    {
        candidate.optimistic_header = update.attested_header.clone();
    }
    let finalized_next = !is_next_sync_committee_known(&candidate) &&
        is_sync_committee_update(update) &&
        is_finality_update(update) &&
        period(update.finalized_header.beacon().slot, params) ==
            period(update.attested_header.beacon().slot, params);
    if count * 3 >= update.sync_aggregate.sync_committee_bits.bits.len() as u64 * 2 &&
        (update.finalized_header.beacon().slot > candidate.finalized_header.beacon().slot ||
            finalized_next)
    {
        apply_update(&mut candidate, update, params)?;
        candidate.best_valid_update = None;
    }
    *store = candidate;
    Ok(())
}
/// Explicit optional timeout policy; only promotes an update already verified by this store.
pub fn process_light_client_store_force_update(
    store: &mut LightClientStore,
    current_slot: u64,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    params.validate()?;
    if &store.parameters != params {
        return Err(LightClientError::Invalid(
            "store parameters differ from authenticated bootstrap",
        ))
    }
    let timeout = params
        .slots_per_epoch()
        .checked_mul(params.epochs_per_sync_committee_period())
        .ok_or(LightClientError::Invalid("update timeout overflow"))?;
    let elapsed = current_slot
        .checked_sub(store.finalized_header.beacon().slot)
        .is_some_and(|gap| gap > timeout);
    if elapsed && let Some(mut update) = store.best_valid_update.clone() {
        let mut candidate = store.clone();
        if update.finalized_header.beacon().slot <= store.finalized_header.beacon().slot {
            update.finalized_header = update.attested_header.clone()
        }
        apply_update(&mut candidate, &update, params)?;
        candidate.best_valid_update = None;
        *store = candidate;
    }
    Ok(())
}
/// Expand and verify a compact finality update with the same mandatory full-update rules.
pub fn process_light_client_finality_update(
    store: &mut LightClientStore,
    update: &FinalityUpdate,
    current_slot: u64,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    let (committee_depth, _) = branch_depths(update.schema)?;
    process_light_client_update(
        store,
        &Update {
            schema: update.schema,
            attested_header: update.attested_header.clone(),
            next_sync_committee: empty_committee(params.preset()),
            next_sync_committee_branch: vec![[0; 32]; committee_depth],
            finalized_header: update.finalized_header.clone(),
            finality_branch: update.finality_branch.clone(),
            sync_aggregate: update.sync_aggregate.clone(),
            signature_slot: update.signature_slot,
        },
        current_slot,
        params,
    )
}
/// Expand and verify a compact optimistic update with the same mandatory full-update rules.
pub fn process_light_client_optimistic_update(
    store: &mut LightClientStore,
    update: &OptimisticUpdate,
    current_slot: u64,
    params: &LightClientParameters,
) -> Result<(), LightClientError> {
    let (committee_depth, finality_depth) = branch_depths(update.schema)?;
    process_light_client_update(
        store,
        &Update {
            schema: update.schema,
            attested_header: update.attested_header.clone(),
            next_sync_committee: empty_committee(params.preset()),
            next_sync_committee_branch: vec![[0; 32]; committee_depth],
            finalized_header: super::headers::empty_header(update.schema, params.preset())?,
            finality_branch: vec![[0; 32]; finality_depth],
            sync_aggregate: update.sync_aggregate.clone(),
            signature_slot: update.signature_slot,
        },
        current_slot,
        params,
    )
}
