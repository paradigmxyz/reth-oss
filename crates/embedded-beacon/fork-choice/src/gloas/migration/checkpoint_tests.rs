//! Real signed-deposit checkpoints, empty-slot replay and signed Gloas continuation controls.
use super::*;

fn checkpoint_classic(
    selected: Fork,
    target: u64,
) -> (Store, Vec<SecretKey>, TransitionParameters, AnyState) {
    let (store, keys, mut transitions) = real_anchor(selected);
    if selected == Fork::Phase0 {
        for activation in &mut transitions.schedule {
            if activation.fork > Fork::Phase0 {
                activation.epoch = u64::MAX;
            }
        }
        transitions = TransitionParameters::new(transitions.gloas, transitions.schedule).unwrap();
    }
    let root = store.get_head().unwrap().root;
    let original = store.block_state(root).unwrap();
    let boundary = if target == original.slot() {
        original.clone()
    } else {
        fork_processing::replay_slots(original, target, &transitions, target).unwrap()
    };
    (store, keys, transitions, boundary)
}

#[test]
fn trusted_skipped_classic_boundary_keeps_actual_original_commitment_and_has_real_boundary() {
    let (original, _, transitions, boundary) = checkpoint_classic(Fork::Phase0, 16);
    let root = original.get_head().unwrap().root;
    let post = original.block_state_arc(root).unwrap();
    let block = original.block_arc(root).unwrap();
    let checkpoint = Checkpoint { epoch: 2, root };
    let store = Store::get_store_at_checkpoint(
        post.as_ref().clone(),
        block.as_ref().clone(),
        checkpoint,
        &boundary.to_ssz().unwrap(),
        original.migration_parameters(),
        transitions,
    )
    .unwrap();
    assert_eq!(store.checkpoints(), (checkpoint, checkpoint));
    assert_eq!(store.current_slot(), 16);
    assert_eq!(store.snapshot().unwrap().head_slot, 0);
    assert_eq!(
        store.block(root).unwrap().header().unwrap().state_root,
        post.hash_tree_root().unwrap()
    );
    assert_eq!(store.block_state(root).unwrap(), post.as_ref());
    assert_eq!(store.block_state(root).unwrap().slot(), 0);
    let replayed = FastConfirmationView::checkpoint_state(&store, checkpoint).unwrap();
    let StateRef::Classic(replayed) = replayed else { panic!("genuine classic boundary") };
    assert_eq!(replayed.to_ssz().unwrap(), boundary.to_ssz().unwrap());
    assert_eq!(replayed.hash_tree_root().unwrap(), boundary.hash_tree_root().unwrap());
    assert_ne!(replayed.hash_tree_root().unwrap(), post.hash_tree_root().unwrap());
    assert_eq!(FastConfirmationView::unrealized_justified(&store), checkpoint);
    assert_eq!(store.get_head().unwrap().payload_status, None);
}

#[test]
fn fulu_origin_gloas_empty_boundary_preserves_history_and_imports_actual_signed_gloas_child() {
    let (original, keys, transitions, boundary) = checkpoint_classic(Fork::Fulu, 8);
    assert_eq!(boundary.schema_fork(), Fork::Gloas);
    let root = original.get_head().unwrap().root;
    let post = original.block_state_arc(root).unwrap();
    let block = original.block_arc(root).unwrap();
    let checkpoint = Checkpoint { epoch: 1, root };
    let mut store = Store::get_store_at_checkpoint(
        post.as_ref().clone(),
        block.as_ref().clone(),
        checkpoint,
        &boundary.to_ssz().unwrap(),
        original.migration_parameters(),
        transitions.clone(),
    )
    .unwrap();
    assert_eq!(store.block_state(root).unwrap().schema_fork(), Fork::Fulu);
    let StateRef::Classic(checkpoint_state) =
        FastConfirmationView::checkpoint_state(&store, checkpoint).unwrap()
    else {
        panic!("original classic graph keeps the separately typed upgraded boundary")
    };
    assert_eq!(checkpoint_state.schema_fork(), Fork::Gloas);
    assert_eq!(checkpoint_state.slot(), 8);
    assert_eq!(store.checkpoints(), (checkpoint, checkpoint));
    store.on_tick(9 * 6000).unwrap();
    let child = signed_first_at(&store, root, &keys, &transitions, 9);
    let child_root = child.message_root().unwrap();
    let engine = UnavailableEngine(AtomicUsize::new(0));
    let migrated = ready(GloasStore::migrate_from_classic(&store, &child, &engine)).unwrap();
    assert_eq!(engine.0.load(Ordering::SeqCst), 0);
    assert_eq!(migrated.historical_block_state_arc(root).unwrap().as_ref(), post.as_ref());
    assert_eq!(migrated.historical_block_arc(root).unwrap().as_ref(), block.as_ref());
    assert_eq!(migrated.block_state(child_root).unwrap().slot, 9);
    assert_eq!(migrated.finalized, checkpoint);
    assert_eq!(migrated.justified, checkpoint);
    assert_eq!(migrated.snapshot().unwrap().head_slot, 9);
    assert_eq!(
        migrated
            .verified_execution_block_hash(ForkChoiceNode { root, payload_status: None })
            .unwrap(),
        None
    );
}

#[test]
fn exact_original_boundary_reuses_its_original_state_and_rejects_changed_checkpoint_root() {
    let (original, _, transitions, boundary) = checkpoint_classic(Fork::Phase0, 0);
    let root = original.get_head().unwrap().root;
    let checkpoint = Checkpoint { epoch: 0, root };
    let store = Store::get_store_at_checkpoint(
        original.block_state(root).unwrap().clone(),
        original.block(root).unwrap().clone(),
        checkpoint,
        &boundary.to_ssz().unwrap(),
        original.migration_parameters(),
        transitions.clone(),
    )
    .unwrap();
    let StateRef::Classic(boundary_read) =
        FastConfirmationView::checkpoint_state(&store, checkpoint).unwrap()
    else {
        panic!("actual classic checkpoint")
    };
    assert!(std::ptr::eq(store.block_state(root).unwrap(), boundary_read));
    assert!(matches!(
        Store::get_store_at_checkpoint(
            original.block_state(root).unwrap().clone(),
            original.block(root).unwrap().clone(),
            Checkpoint { epoch: 0, root: [99; 32] },
            &boundary.to_ssz().unwrap(),
            original.migration_parameters(),
            transitions,
        ),
        Err(StoreError::AnchorMismatch)
    ));
}

#[test]
fn skipped_boundary_changed_bytes_future_root_and_resource_bombs_cannot_build_authority() {
    let (original, _, transitions, boundary) = checkpoint_classic(Fork::Phase0, 16);
    let root = original.get_head().unwrap().root;
    let before = original.block_state(root).unwrap().to_ssz().unwrap();
    let mut bytes = boundary.to_ssz().unwrap();
    bytes[0] ^= 1;
    let attempt = |checkpoint, data: &[u8], limits, parameters| {
        Store::get_store_at_checkpoint(
            original.block_state(root).unwrap().clone(),
            original.block(root).unwrap().clone(),
            checkpoint,
            data,
            limits,
            parameters,
        )
    };
    let checkpoint = Checkpoint { epoch: 2, root };
    assert!(matches!(
        attempt(checkpoint, &bytes, original.migration_parameters(), transitions.clone()),
        Err(StoreError::AnchorMismatch)
    ));
    assert!(matches!(
        attempt(checkpoint, &before, original.migration_parameters(), transitions.clone()),
        Err(StoreError::AnchorMismatch)
    ));
    let mut limits = original.migration_parameters();
    limits.max_tick_slots = 15;
    assert!(matches!(
        attempt(checkpoint, &boundary.to_ssz().unwrap(), limits, transitions.clone()),
        Err(StoreError::ResourceLimit)
    ));
    assert!(matches!(
        attempt(
            Checkpoint { epoch: u64::MAX, root },
            &[],
            original.migration_parameters(),
            transitions.clone()
        ),
        Err(StoreError::Overflow)
    ));
    let mut invalid = transitions;
    invalid.schedule[1].version = invalid.schedule[0].version;
    assert!(attempt(
        checkpoint,
        &boundary.to_ssz().unwrap(),
        original.migration_parameters(),
        invalid
    )
    .is_err());
    assert_eq!(original.block_state(root).unwrap().to_ssz().unwrap(), before);
    assert_eq!(original.checkpoints().0.epoch, 0);
}

fn actual_gloas_poststate() -> (GloasStore, Root) {
    let (mut original, keys, transitions) = real_anchor(Fork::Fulu);
    original.on_tick(8 * 6000).unwrap();
    let signed = signed_first(&original, &keys, &transitions);
    let root = signed.message_root().unwrap();
    let engine = UnavailableEngine(AtomicUsize::new(0));
    let store = ready(GloasStore::migrate_from_classic(&original, &signed, &engine)).unwrap();
    assert_eq!(engine.0.load(Ordering::SeqCst), 0);
    (store, root)
}

#[test]
fn trusted_gloas_skipped_boundary_retains_real_signed_blocks_committed_original_poststate() {
    let (original, root) = actual_gloas_poststate();
    let post = original.block_state_arc(root).unwrap();
    let block = original.block_arc(root).unwrap();
    let boundary = gloas_processing::replay_slots(&post, 16, &original.processing, 8).unwrap();
    let checkpoint = Checkpoint { epoch: 2, root };
    let store = GloasStore::get_store_at_checkpoint(
        post.as_ref().clone(),
        block.as_ref().clone(),
        checkpoint,
        &boundary.to_ssz(Preset::Minimal).unwrap(),
        original.parameters,
        original.processing,
    )
    .unwrap();
    assert_eq!(store.snapshot().unwrap().head_slot, 8);
    assert_eq!(store.current_slot(), 16);
    assert_eq!(store.justified, checkpoint);
    assert_eq!(store.finalized, checkpoint);
    assert_eq!(store.unrealized_justified, checkpoint);
    assert_eq!(store.unrealized_finalized, checkpoint);
    assert_eq!(store.block_state(root).unwrap(), post.as_ref());
    assert_eq!(
        store.block(root).unwrap().state_root,
        post.hash_tree_root(Preset::Minimal).unwrap()
    );
    let StateRef::Gloas(replayed, _) =
        FastConfirmationView::checkpoint_state(&store, checkpoint).unwrap()
    else {
        panic!("actual Gloas boundary")
    };
    assert_eq!(
        replayed.to_ssz(Preset::Minimal).unwrap(),
        boundary.to_ssz(Preset::Minimal).unwrap()
    );
    assert_ne!(replayed.hash_tree_root(Preset::Minimal).unwrap(), block.state_root);
    assert!(!store.is_payload_verified(root));
}

#[test]
fn gloas_boundary_requires_exact_replay_bytes_and_bounded_gap() {
    let (original, root) = actual_gloas_poststate();
    let post = original.block_state_arc(root).unwrap();
    let block = original.block_arc(root).unwrap();
    let boundary = gloas_processing::replay_slots(&post, 16, &original.processing, 8).unwrap();
    let checkpoint = Checkpoint { epoch: 2, root };
    let mut bytes = boundary.to_ssz(Preset::Minimal).unwrap();
    bytes[0] ^= 1;
    assert!(matches!(
        GloasStore::get_store_at_checkpoint(
            post.as_ref().clone(),
            block.as_ref().clone(),
            checkpoint,
            &bytes,
            original.parameters,
            original.processing.clone(),
        ),
        Err(StoreError::AnchorMismatch)
    ));
    let mut limits = original.parameters;
    limits.max_tick_slots = 7;
    assert!(matches!(
        GloasStore::get_store_at_checkpoint(
            post.as_ref().clone(),
            block.as_ref().clone(),
            checkpoint,
            &boundary.to_ssz(Preset::Minimal).unwrap(),
            limits,
            original.processing.clone(),
        ),
        Err(StoreError::ResourceLimit)
    ));
    assert!(matches!(
        GloasStore::get_store_at_checkpoint(
            post.as_ref().clone(),
            block.as_ref().clone(),
            Checkpoint { epoch: 0, root },
            &[],
            original.parameters,
            original.processing.clone(),
        ),
        Err(StoreError::AnchorMismatch)
    ));
    assert_eq!(original.block_state(root).unwrap(), post.as_ref());
}
