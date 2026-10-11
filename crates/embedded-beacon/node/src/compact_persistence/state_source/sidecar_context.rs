//! Independent original-block ownership for retained sidecar proof contexts.
//!
//! Durable files are data-only on reopen. Only a sealed native admitted/finalized receipt can
//! authenticate their ancestry and proposer keys; this archive never inserts fork-choice roots.

use super::*;
use crate::compact_persistence::retention_gc::RetentionDomain;
use reth_embedded_beacon_network::block_rpc::{BlockRequest, WireBlock, MAX_BATCH_BYTES};
use reth_embedded_beacon_shared_data::journal::{ensure_published_durability, publish_file};
use reth_fs_util as fs;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

const RETENTION_EPOCHS: u64 = 4096;
const MAX_CONTEXT_BYTES: u64 = 4096;
const MAX_EPOCH_DIRECTORIES: usize = 4098;

/// A source-owned retained context; fields and construction stay inside native receipt handling.
#[derive(Debug)]
pub(crate) struct SidecarContexts {
    directory: PathBuf,
    index: BTreeMap<Root, u64>,
    verified_keys: BTreeMap<Root, [u8; 48]>,
    selected: BTreeMap<u64, Root>,
    _writer: File,
    poisoned: bool,
    last_anchor: Option<Root>,
    staged_snapshot: Option<StoreSnapshot>,
    staged_selected: BTreeMap<u64, Root>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextRecord {
    schema: u8,
    identity: StoreIdentity,
    consensus_digest: Root,
    root: Root,
    slot: u64,
    parent: Root,
    object: ObjectId,
    checksum: Root,
}

impl SidecarContexts {
    pub(crate) fn open(repository: &AdmittedStateRepository) -> Result<Self, PersistenceError> {
        let directory = repository.shared.namespace_root().join("beacon-sidecar-contexts-v1");
        fs::create_dir_all(&directory).map_err(SharedError::from)?;
        let path = directory.join("writer.lock");
        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| SharedError::Fs(fs::FsPathError::open(error, &path)))?;
        writer.try_lock().map_err(|_| SharedError::AlreadyOpen(path))?;
        RetentionDomain::Contexts
            .resume(&repository.shared, repository.network.consensus_digest)?;
        let mut result = Self {
            directory,
            index: BTreeMap::new(),
            verified_keys: BTreeMap::new(),
            selected: BTreeMap::new(),
            _writer: writer,
            poisoned: false,
            last_anchor: None,
            staged_snapshot: None,
            staged_selected: BTreeMap::new(),
        };
        let mut epochs = 0;
        for entry in fs::read_dir(&result.directory).map_err(SharedError::from)? {
            let entry = entry.map_err(|_| PersistenceError::Linkage)?;
            if entry.file_name().to_str().is_some_and(RetentionDomain::is_operational_file) &&
                entry.file_type().map_err(|_| PersistenceError::Linkage)?.is_file()
            {
                continue
            }
            epochs += 1;
            if epochs > MAX_EPOCH_DIRECTORIES ||
                !entry.file_type().map_err(|_| PersistenceError::Linkage)?.is_dir()
            {
                return Err(PersistenceError::Capacity)
            }
            let name = entry.file_name();
            let text = name.to_str().ok_or(PersistenceError::Linkage)?;
            let epoch = text.parse::<u64>().map_err(|_| PersistenceError::Linkage)?;
            if text != format!("{epoch:020}") {
                return Err(PersistenceError::Linkage)
            }
            let mut count = 0u64;
            let mut records = 0u64;
            for file in fs::read_dir(entry.path()).map_err(SharedError::from)? {
                let file = file.map_err(|_| PersistenceError::Linkage)?;
                count += 1;
                if count > repository.network.config.slots_per_epoch.saturating_mul(4) ||
                    !file.file_type().map_err(|_| PersistenceError::Linkage)?.is_file()
                {
                    return Err(PersistenceError::Capacity)
                }
                let name = file.file_name();
                let name = name.to_str().ok_or(PersistenceError::Linkage)?;
                // A crash before the atomic rename can leave bounded unpublished bytes.
                // They carry no receipt and must neither block recovery nor select an ancestor.
                if let Some(root) = name.strip_suffix(".intent") {
                    let root = decode_context_name(root)?;
                    if fs::metadata(file.path()).map_err(SharedError::from)?.len() != 0 {
                        return Err(PersistenceError::Linkage)
                    }
                    if !file.path().with_extension("json").exists() {
                        repository.shared.release_owners(&[context_owner(root)])?;
                    }
                    fs::remove_file(file.path()).map_err(SharedError::from)?;
                    continue
                }
                if let Some(root) = name.strip_suffix(".pending") {
                    if root.len() != 64 ||
                        !root
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) ||
                        fs::metadata(file.path()).map_err(SharedError::from)?.len() >
                            MAX_CONTEXT_BYTES
                    {
                        return Err(PersistenceError::Linkage)
                    }
                    continue
                }
                records += 1;
                if records > repository.network.config.slots_per_epoch.saturating_mul(2) {
                    return Err(PersistenceError::Capacity)
                }
                let record = result.read_record(repository, &file.path())?;
                if record.slot / repository.network.config.slots_per_epoch != epoch ||
                    file.file_name().to_str() != Some(&context_name(record.root)) ||
                    result.index.insert(record.root, record.slot).is_some()
                {
                    return Err(PersistenceError::Linkage)
                }
            }
        }
        // Integrity and pin checks above confer no native validity or branch-selection receipt.
        Ok(result)
    }

    fn path(&self, slots_per_epoch: u64, slot: u64, root: Root) -> PathBuf {
        self.directory.join(format!("{:020}", slot / slots_per_epoch)).join(context_name(root))
    }
    fn read_record(
        &self,
        repository: &AdmittedStateRepository,
        path: &Path,
    ) -> Result<ContextRecord, PersistenceError> {
        if fs::metadata(path).map_err(SharedError::from)?.len() > MAX_CONTEXT_BYTES {
            return Err(PersistenceError::Capacity)
        }
        let mut record: ContextRecord =
            serde_json::from_slice(&fs::read(path).map_err(SharedError::from)?)?;
        let expected = record.checksum;
        record.checksum = [0; 32];
        if record.schema != 1 ||
            record.identity != repository.shared.identity() ||
            record.consensus_digest != repository.network.consensus_digest ||
            record.object.kind != ObjectKind::BeaconBlock ||
            record.object.encoding != Encoding::Ssz ||
            sha256(&serde_json::to_vec(&record)?) != expected ||
            !repository.shared.owner_objects(context_owner(record.root)).contains(&record.object)
        {
            return Err(PersistenceError::Linkage)
        }
        record.checksum = expected;
        Ok(record)
    }
    fn original(
        &self,
        repository: &AdmittedStateRepository,
        root: Root,
    ) -> Result<Option<(WireBlock, ContextRecord)>, PersistenceError> {
        let Some(slot) = self.index.get(&root).copied() else { return Ok(None) };
        let record = self.read_record(
            repository,
            &self.path(repository.network.config.slots_per_epoch, slot, root),
        )?;
        let bytes = repository
            .shared
            .get_bounded(record.object, repository.network.config.max_payload_size)?
            .ok_or(PersistenceError::Linkage)?;
        let wire = WireBlock::from_ssz(bytes.to_vec(), &repository.network.config)
            .map_err(|_| PersistenceError::Linkage)?;
        if wire.root() != root || wire.slot() != record.slot || wire.parent_root() != record.parent
        {
            return Err(PersistenceError::Linkage)
        }
        Ok(Some((wire, record)))
    }
    fn retain(
        &mut self,
        repository: &AdmittedStateRepository,
        admitted: &AdmittedState<'_>,
    ) -> Result<(), PersistenceError> {
        self.healthy(repository)?;
        let binding = admitted.binding();
        if binding.identity().profile_digest != repository.network.profile_digest ||
            binding.identity().consensus_config_digest != repository.network.consensus_digest ||
            binding.state_root() != binding.origin_state_root() ||
            binding.checkpoint_epoch().is_some() ||
            binding.fork() != admitted.state().fork() ||
            binding.preset() != admitted.state().preset() ||
            binding.genesis_time() != admitted.state().genesis_time() ||
            binding.genesis_validators_root() != admitted.state().genesis_validators_root() ||
            binding.slot() != admitted.state().slot() ||
            binding.origin_block_root() != admitted.signed_block().message_root()?
        {
            return Err(PersistenceError::Identity)
        }
        // The sealed receipt binds this immutable post-state to its fully verified root. The
        // admission path checked that root, and complete-state persistence checks it again before
        // publishing the durable record; constructing the proof context needs no third full hash.
        let bytes = admitted.signed_block().to_ssz()?;
        repository.check_bytes(&bytes)?;
        let wire = WireBlock::from_ssz(bytes, &repository.network.config)
            .map_err(|_| PersistenceError::Linkage)?;
        let key = state_key(admitted.state(), proposer_index(&wire)?)?;
        verify_key(repository, &wire, key)?;
        self.persist_wire(repository, &wire, key)
    }
    fn persist_wire(
        &mut self,
        repository: &AdmittedStateRepository,
        wire: &WireBlock,
        key: [u8; 48],
    ) -> Result<(), PersistenceError> {
        self.healthy(repository)?;
        verify_key(repository, wire, key)?;
        let bytes = wire.ssz();
        repository.check_bytes(bytes)?;
        let object = ObjectId::new(ObjectKind::BeaconBlock, Encoding::Ssz, bytes);
        let path = self.path(repository.network.config.slots_per_epoch, wire.slot(), wire.root());
        if let Some(prior_slot) = self.index.get(&wire.root()) {
            if *prior_slot != wire.slot() {
                return Err(PersistenceError::Linkage)
            }
            let previous = self.read_record(repository, &path)?;
            if previous.object != object || previous.parent != wire.parent_root() {
                return Err(PersistenceError::Linkage)
            }
            ensure_published_durability(&path)?;
        } else {
            let epoch = wire.slot() / repository.network.config.slots_per_epoch;
            let in_epoch = self
                .index
                .values()
                .filter(|slot| **slot / repository.network.config.slots_per_epoch == epoch)
                .count();
            if in_epoch as u64 >= repository.network.config.slots_per_epoch.saturating_mul(2) {
                return Err(PersistenceError::Capacity)
            }
            fs::create_dir_all(path.parent().ok_or(PersistenceError::Linkage)?)
                .map_err(SharedError::from)?;
            let intent = path.with_extension("intent");
            publish_file(&intent, &[])?;
            repository.shared.put(
                ObjectKind::BeaconBlock,
                Encoding::Ssz,
                bytes,
                context_owner(wire.root()),
            )?;
            let mut record = ContextRecord {
                schema: 1,
                identity: repository.shared.identity(),
                consensus_digest: repository.network.consensus_digest,
                root: wire.root(),
                slot: wire.slot(),
                parent: wire.parent_root(),
                object,
                checksum: [0; 32],
            };
            record.checksum = sha256(&serde_json::to_vec(&record)?);
            let bytes = serde_json::to_vec(&record)?;
            if bytes.len() as u64 > MAX_CONTEXT_BYTES {
                return Err(PersistenceError::Capacity)
            }
            publish_file(&path, &bytes)?;
            fs::remove_file(&intent).map_err(SharedError::from)?;
            self.index.insert(wire.root(), wire.slot());
        }
        self.verified_keys.insert(wire.root(), key);
        Ok(())
    }
    fn authenticate(
        &mut self,
        repository: &AdmittedStateRepository,
        anchor: Root,
        state: StateData<'_>,
    ) -> Result<(), PersistenceError> {
        self.healthy(repository)?;
        let horizon = RETENTION_EPOCHS
            .checked_mul(repository.network.config.slots_per_epoch)
            .ok_or(PersistenceError::Capacity)?;
        let floor = state.slot().saturating_sub(horizon);
        let mut root = anchor;
        let mut previous_slot = None;
        let maximum =
            horizon.checked_add(repository.max_records as u64).ok_or(PersistenceError::Capacity)?;
        for _ in 0..maximum {
            let Some((wire, _)) = self.original(repository, root)? else { break };
            if wire.slot() < floor {
                break
            }
            if previous_slot.is_some_and(|slot| wire.slot() >= slot) {
                return Err(PersistenceError::Linkage)
            }
            let key = state_key(state, proposer_index(&wire)?)?;
            verify_key(repository, &wire, key)?;
            self.verified_keys.insert(root, key);
            if let Some(prior) = self.selected.get(&wire.slot()).copied() {
                if prior != root {
                    return Err(PersistenceError::Linkage)
                }
                break
            }
            if self.staged_selected.insert(wire.slot(), root).is_some_and(|prior| prior != root) {
                return Err(PersistenceError::Linkage)
            }
            previous_slot = Some(wire.slot());
            if wire.parent_root() == [0; 32] {
                break
            }
            root = wire.parent_root();
        }
        Ok(())
    }
    fn prune(
        &mut self,
        repository: &AdmittedStateRepository,
        floor: u64,
    ) -> Result<(), PersistenceError> {
        let floor_epoch = floor / repository.network.config.slots_per_epoch;
        for entry in fs::read_dir(&self.directory).map_err(SharedError::from)? {
            let entry = entry.map_err(|_| PersistenceError::Linkage)?;
            if entry.file_name().to_str().is_some_and(RetentionDomain::is_operational_file) {
                continue
            }
            let name = entry.file_name();
            let name = name.to_str().ok_or(PersistenceError::Linkage)?;
            let epoch = name.parse::<u64>().map_err(|_| PersistenceError::Linkage)?;
            if name != format!("{epoch:020}") ||
                !entry.file_type().map_err(|_| PersistenceError::Linkage)?.is_dir()
            {
                return Err(PersistenceError::Linkage)
            }
            if epoch >= floor_epoch {
                continue
            }
            let mut roots = BTreeSet::new();
            let mut count = 0u64;
            for file in fs::read_dir(entry.path()).map_err(SharedError::from)? {
                let file = file.map_err(|_| PersistenceError::Linkage)?;
                count += 1;
                if count > repository.network.config.slots_per_epoch.saturating_mul(4) ||
                    !file.file_type().map_err(|_| PersistenceError::Linkage)?.is_file()
                {
                    return Err(PersistenceError::Capacity)
                }
                let name = file.file_name();
                let name = name.to_str().ok_or(PersistenceError::Linkage)?;
                let root = name
                    .strip_suffix(".json")
                    .or_else(|| name.strip_suffix(".pending"))
                    .or_else(|| name.strip_suffix(".intent"))
                    .ok_or(PersistenceError::Linkage)?;
                roots.insert(decode_context_name(root)?);
            }
            RetentionDomain::Contexts.prune_epoch(
                &repository.shared,
                repository.network.consensus_digest,
                epoch,
                roots.into_iter().collect(),
            )?;
        }
        self.index
            .retain(|_, slot| *slot / repository.network.config.slots_per_epoch >= floor_epoch);
        self.verified_keys.retain(|root, _| self.index.contains_key(root));
        self.selected
            .retain(|slot, _| *slot / repository.network.config.slots_per_epoch >= floor_epoch);
        Ok(())
    }
    fn serve(
        &self,
        repository: &AdmittedStateRepository,
        request: &BlockRequest,
    ) -> Result<Option<Vec<WireBlock>>, PersistenceError> {
        self.healthy(repository)?;
        if request.validate_outbound().is_err() {
            return Ok(None)
        }
        let roots = match request {
            BlockRequest::Roots(roots) => roots.clone(),
            BlockRequest::Range { start_slot, count } => {
                let end = start_slot.checked_add(*count).ok_or(PersistenceError::Capacity)?;
                self.selected.range(*start_slot..end).map(|(_, root)| *root).collect()
            }
        };
        let mut result = Vec::new();
        let mut bytes = 0usize;
        for root in roots {
            let Some(key) = self.verified_keys.get(&root).copied() else { continue };
            let Some((wire, _)) = self.original(repository, root)? else {
                return Err(PersistenceError::Linkage)
            };
            verify_key(repository, &wire, key)?;
            bytes = bytes.checked_add(wire.ssz().len()).ok_or(PersistenceError::Capacity)?;
            if bytes > MAX_BATCH_BYTES {
                return Ok(None)
            }
            result.push(wire);
        }
        request.matches(&result).map_err(|_| PersistenceError::Linkage)?;
        Ok(Some(result))
    }
    fn stage(&mut self, snapshot: StoreSnapshot) {
        if self.staged_snapshot != Some(snapshot) {
            self.staged_selected.clear();
            self.staged_snapshot = Some(snapshot);
        }
    }
    fn commit(
        &mut self,
        repository: &AdmittedStateRepository,
        actual: StoreSnapshot,
    ) -> Result<(), PersistenceError> {
        self.healthy(repository)?;
        if self.staged_snapshot != Some(actual) {
            return Err(PersistenceError::Linkage)
        }
        for (slot, root) in std::mem::take(&mut self.staged_selected) {
            if self.selected.insert(slot, root).is_some_and(|prior| prior != root) {
                return Err(PersistenceError::Linkage)
            }
        }
        self.staged_snapshot = None;
        self.last_anchor = Some(actual.finalized_checkpoint.root);
        let horizon = RETENTION_EPOCHS
            .checked_mul(repository.network.config.slots_per_epoch)
            .ok_or(PersistenceError::Capacity)?;
        self.prune(repository, actual.head_slot.saturating_sub(horizon))
    }
    fn healthy(&self, repository: &AdmittedStateRepository) -> Result<(), PersistenceError> {
        if self.poisoned {
            return Err(PersistenceError::CoordinatorPoisoned)
        }
        repository.shared.ensure_healthy()?;
        Ok(())
    }
}

impl RepositoryStateSource {
    pub(crate) fn retain_context(
        &self,
        admitted: &AdmittedState<'_>,
    ) -> Result<(), PersistenceError> {
        let mut contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        let result = contexts.retain(&self.repository, admitted);
        if result.is_err() {
            contexts.poisoned = true
        }
        result
    }
    pub(crate) fn finalize_context(
        &self,
        admitted: &FinalizedRetention<'_>,
    ) -> Result<(), PersistenceError> {
        let mut contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        contexts.stage(admitted.actual_snapshot());
        let result = contexts.authenticate(
            &self.repository,
            admitted.boundary_checkpoint().root,
            admitted.boundary_state(),
        );
        if result.is_err() {
            contexts.poisoned = true
        }
        result
    }
    /// Reopen data-only ancestry from the exact actual native finalized root and current registry.
    /// This sealed receipt is the sole authority; saved canonical fields are never consulted.
    pub(crate) fn restore_context_anchor(
        &self,
        admitted: &AdmittedSnapshot,
    ) -> Result<(), PersistenceError> {
        if admitted.identity() != self.identity() {
            return Err(PersistenceError::Identity)
        }
        let actual = admitted.snapshot();
        if !admitted.retained_roots().contains(&actual.head.root) ||
            !admitted.retained_roots().contains(&actual.finalized_checkpoint.root)
        {
            return Err(PersistenceError::Linkage)
        }
        let mut contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        contexts.healthy(&self.repository)?;
        contexts.stage(actual);
        if contexts.last_anchor == Some(actual.finalized_checkpoint.root) {
            return Ok(())
        }
        // The root of this complete immutable post-state is already bound to the exact native
        // graph head by the sealed receipt and repository's original block/state commitment.
        let lease =
            self.repository.block_state(actual.head.root)?.ok_or(PersistenceError::Linkage)?;
        let state = StateData::Classic(lease.state());
        let anchor = actual.finalized_checkpoint.root;
        let result = if contexts.index.contains_key(&anchor) {
            contexts.authenticate(&self.repository, anchor, state)
        } else {
            // Trusted unsigned anchors remain unsigned. Their exact native root authenticates
            // the immutable parent commitment; only original signed ancestors become responses.
            let original =
                self.repository.original_block(anchor)?.ok_or(PersistenceError::Linkage)?;
            let header = match original.0 {
                CheckpointBlockFormat::Signed => {
                    let wire =
                        WireBlock::from_ssz(original.1.to_vec(), &self.repository.network.config)
                            .map_err(|_| PersistenceError::Linkage)?;
                    if wire.root() != anchor {
                        return Err(PersistenceError::Linkage)
                    }
                    let signed = SignedBeaconBlock::from_ssz(
                        wire.ssz(),
                        wire.fork(),
                        self.repository.network.preset(),
                    )?;
                    signed.header()?
                }
                CheckpointBlockFormat::Unsigned => {
                    let bytes = original.1;
                    let slot = u64::from_le_bytes(
                        bytes
                            .get(..8)
                            .ok_or(PersistenceError::Linkage)?
                            .try_into()
                            .map_err(|_| PersistenceError::Linkage)?,
                    );
                    let fork = self
                        .repository
                        .network
                        .config
                        .fork_at_slot(slot)
                        .map_err(|_| PersistenceError::Identity)?
                        .fork;
                    let block =
                        BeaconBlock::from_ssz(&bytes, fork, self.repository.network.preset())?;
                    if block.hash_tree_root()? != anchor {
                        return Err(PersistenceError::Linkage)
                    }
                    block.header()?
                }
            };
            contexts.authenticate(&self.repository, header.parent_root, state)
        };
        if result.is_err() {
            contexts.poisoned = true
        }
        result
    }

    /// Publish selected serving ancestry only after the actual canonical Engine/durable commit.
    pub(crate) fn commit_context_snapshot(
        &self,
        actual: StoreSnapshot,
    ) -> Result<(), PersistenceError> {
        let mut contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        let result = contexts.commit(&self.repository, actual);
        if result.is_err() {
            contexts.poisoned = true
        }
        result
    }
    /// Failed or pending canonical publication cannot change selected serving ancestry.
    pub(crate) fn discard_context_snapshot(&self) -> Result<(), PersistenceError> {
        let mut contexts =
            self.sidecar_contexts.lock().map_err(|_| PersistenceError::CoordinatorPoisoned)?;
        contexts.healthy(&self.repository)?;
        contexts.staged_selected.clear();
        contexts.staged_snapshot = None;
        Ok(())
    }
    /// Exact original blocks authenticated by a native receipt, independent of hot graph pruning.
    pub(crate) fn serving_contexts(
        &self,
        request: &BlockRequest,
    ) -> Result<Option<Vec<WireBlock>>, PersistenceError> {
        self.sidecar_contexts
            .lock()
            .map_err(|_| PersistenceError::CoordinatorPoisoned)?
            .serve(&self.repository, request)
    }
}

fn state_key(state: StateData<'_>, proposer: u64) -> Result<[u8; 48], PersistenceError> {
    let index = usize::try_from(proposer).map_err(|_| PersistenceError::Linkage)?;
    let registry = match state {
        StateData::Classic(state) => validators(state),
        StateData::Gloas(state, _) => &state.validators.values,
    };
    registry.get(index).map(|validator| validator.pubkey).ok_or(PersistenceError::Linkage)
}
fn verify_key(
    repository: &AdmittedStateRepository,
    wire: &WireBlock,
    key: [u8; 48],
) -> Result<(), PersistenceError> {
    let version = repository
        .network
        .config
        .fork_at_slot(wire.slot())
        .map_err(|_| PersistenceError::Identity)?
        .version;
    let signature: [u8; 96] = wire
        .ssz()
        .get(4..100)
        .ok_or(PersistenceError::Linkage)?
        .try_into()
        .map_err(|_| PersistenceError::Linkage)?;
    verify_signature(
        &key,
        &compute_signing_root(
            wire.root(),
            compute_domain([0; 4], version, repository.network.config.genesis_validators_root),
        ),
        &signature,
    )?;
    Ok(())
}
fn context_owner(root: Root) -> Owner {
    let mut input = b"embedded-beacon-sidecar-context-v1".to_vec();
    input.extend_from_slice(&root);
    Owner::Recovery(sha256(&input))
}
fn context_name(root: Root) -> String {
    let hex = root.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    format!("{hex}.json")
}

fn proposer_index(wire: &WireBlock) -> Result<u64, PersistenceError> {
    // WireBlock already decoded the complete canonical signed container. These bytes are the
    // fixed native message proposer field immediately after its slot, not a caller root hint.
    Ok(u64::from_le_bytes(
        wire.ssz()
            .get(108..116)
            .ok_or(PersistenceError::Linkage)?
            .try_into()
            .map_err(|_| PersistenceError::Linkage)?,
    ))
}

fn decode_context_name(text: &str) -> Result<Root, PersistenceError> {
    if text.len() != 64 ||
        !text.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PersistenceError::Linkage)
    }
    let mut root = [0; 32];
    for (index, byte) in root.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| PersistenceError::Linkage)?;
    }
    Ok(root)
}

mod history;
