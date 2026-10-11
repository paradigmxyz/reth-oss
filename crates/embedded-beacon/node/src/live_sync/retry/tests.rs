//! Retrieval preference rotation preserves checked peer hints and bounded membership.

use super::*;
use std::collections::HashMap;

#[test]
fn selected_peer_moves_to_end_without_changing_checked_hints() {
    let mut targets = targets(3);
    let peers = targets.iter().map(|target| target.peer).collect::<Vec<_>>();
    let before = inventory(&targets);

    rotate_data_peer(&mut targets, peers[1]);

    assert_eq!(
        targets.iter().map(|target| target.peer).collect::<Vec<_>>(),
        vec![peers[0], peers[2], peers[1]]
    );
    assert_eq!(inventory(&targets), before);
    assert_eq!(targets.len(), before.len());
}

#[test]
fn absent_peer_cannot_change_preference_or_create_a_target() {
    let mut targets = targets(3);
    let order = targets.iter().map(|target| target.peer).collect::<Vec<_>>();
    let before = inventory(&targets);
    let unknown = PeerId::random();
    assert!(!before.contains_key(&unknown));

    rotate_data_peer(&mut targets, unknown);

    assert_eq!(targets.iter().map(|target| target.peer).collect::<Vec<_>>(), order);
    assert_eq!(inventory(&targets), before);
    assert_eq!(targets.len(), before.len());
    let mut empty = Vec::new();
    rotate_data_peer(&mut empty, unknown);
    assert!(empty.is_empty());
}

#[test]
fn successive_data_jobs_visit_every_eligible_peer_before_repeating() {
    let mut targets = targets(MAX_PEERS);
    let original_order = targets.iter().map(|target| target.peer).collect::<Vec<_>>();
    let before = inventory(&targets);
    let mut issued = Vec::new();

    // Each new original has an empty attempted-peer inventory. The successful issue rotates
    // preference so a single provider cannot monopolize successive custody-column jobs.
    for _ in 0..2 * MAX_PEERS {
        let selected = targets
            .iter()
            .find(|target| {
                target.status.head_slot >= 100 && target.earliest.is_none_or(|slot| slot <= 100)
            })
            .unwrap()
            .peer;
        issued.push(selected);
        rotate_data_peer(&mut targets, selected);
        assert_eq!(targets.len(), MAX_PEERS);
        assert_eq!(inventory(&targets), before);
    }

    assert_eq!(&issued[..MAX_PEERS], original_order.as_slice());
    assert_eq!(&issued[MAX_PEERS..], original_order.as_slice());
}

#[test]
fn missing_column_overlap_wins_while_ties_keep_rotation_and_eligibility() {
    let mut targets = targets(5);
    let peers = targets.iter().map(|target| target.peer).collect::<Vec<_>>();
    targets[3].status.head_slot = 99;
    targets[4].earliest = Some(101);
    let scores =
        HashMap::from([(peers[0], 0), (peers[1], 2), (peers[2], 2), (peers[3], 8), (peers[4], 8)]);
    let score = |peer| scores[&peer];
    assert_eq!(select_data_peer(&targets, &[], 100, score), Some(peers[1]));
    rotate_data_peer(&mut targets, peers[1]);
    assert_eq!(select_data_peer(&targets, &[], 100, score), Some(peers[2]));
    assert_eq!(select_data_peer(&targets, &[peers[1], peers[2]], 100, score), Some(peers[0]));
    assert_eq!(select_data_peer(&targets, &[peers[0], peers[1], peers[2]], 100, score), None);
}

#[test]
fn provider_preference_uses_current_remaining_columns_and_preserves_unknown_fallback() {
    let targets = targets(3);
    let peers = targets.iter().map(|target| target.peer).collect::<Vec<_>>();
    let masks =
        HashMap::from([(peers[0], vec![2, 5, 7]), (peers[1], vec![11]), (peers[2], vec![])]);
    let score = |remaining: &[u64], peer| {
        remaining.iter().filter(|column| masks[&peer].contains(column)).count()
    };
    assert_eq!(
        select_data_peer(&targets, &[], 100, |peer| score(&[2, 5, 7, 11], peer)),
        Some(peers[0])
    );
    assert_eq!(select_data_peer(&targets, &[], 100, |peer| score(&[11], peer)), Some(peers[1]));
    assert_eq!(
        select_data_peer(&targets, &[peers[1]], 100, |peer| score(&[11], peer)),
        Some(peers[0])
    );
    assert_eq!(select_data_peer(&targets, &[], 100, |_| 0), Some(peers[0]));
    assert_eq!(select_data_peer(&[], &[], 100, |_| 8), None);
}

#[test]
fn inflated_overlap_empty_provider_cannot_starve_lower_and_unknown_jobs() {
    let mut targets = targets(MAX_PEERS);
    let peers = targets.iter().map(|target| target.peer).collect::<Vec<_>>();
    let candidate = ([7; 32], 100);
    let mut cycle = DataPeerCycle::default();
    let mut selected = Vec::new();
    // Every new empty/partial response creates a new job with an empty tried inventory.
    // The dishonest first peer continues claiming every column but receives one turn per cycle.
    for _ in 0..2 * MAX_PEERS {
        let provider = cycle
            .select(candidate, &targets, &[], |peer| {
                if peer == peers[0] {
                    8
                } else if peer == peers[1] {
                    1
                } else {
                    0
                }
            })
            .unwrap();
        selected.push(provider);
        cycle.issued(provider);
        rotate_data_peer(&mut targets, provider);
        assert!(cycle.issued.len() <= MAX_PEERS);
    }
    for round in selected.chunks(MAX_PEERS) {
        assert_eq!(round[0], peers[0]);
        assert_eq!(round[1], peers[1]);
        assert_eq!(round.iter().copied().collect::<HashSet<_>>().len(), MAX_PEERS);
    }
}

#[test]
fn cycle_changes_only_after_issue_and_preserves_candidate_and_peer_bounds() {
    let mut targets = targets(3);
    let peers = targets.iter().map(|target| target.peer).collect::<Vec<_>>();
    let candidate = ([7; 32], 100);
    let mut cycle = DataPeerCycle::default();
    let score = |peer| if peer == peers[0] { 8 } else { 0 };
    // Local admission pressure must not consume the provider's turn.
    assert_eq!(cycle.select(candidate, &targets, &[], score), Some(peers[0]));
    assert_eq!(cycle.select(candidate, &targets, &[], score), Some(peers[0]));
    assert!(cycle.issued.is_empty());
    cycle.issued(peers[0]);
    assert_eq!(cycle.select(candidate, &targets, &[peers[1]], score), Some(peers[2]));
    cycle.issued(peers[2]);
    targets.retain(|target| target.peer != peers[2]);
    assert_eq!(cycle.select(candidate, &targets, &[], score), Some(peers[1]));
    assert!(!cycle.issued.contains(&peers[2]));
    assert_eq!(cycle.select(([8; 32], 100), &targets, &[], score), Some(peers[0]));
    assert!(cycle.issued.is_empty());
    assert_eq!(cycle.select(([8; 32], 101), &[], &[], score), None);
    assert!(cycle.issued.is_empty());
}

fn targets(count: usize) -> Vec<Target> {
    (0..count)
        .map(|index| Target {
            peer: PeerId::random(),
            status: Status {
                fork_digest: [index as u8; 4],
                finalized_root: [index as u8; 32],
                finalized_epoch: index as u64,
                head_root: [index as u8 + 1; 32],
                head_slot: 100 + index as u64,
            },
            earliest: (index % 2 == 0).then_some(index as u64),
            examined_root: (index % 2 == 1).then_some([index as u8 + 2; 32]),
        })
        .collect()
}

fn inventory(targets: &[Target]) -> HashMap<PeerId, (Status, Option<u64>, Option<Root>)> {
    targets
        .iter()
        .map(|target| (target.peer, (target.status.clone(), target.earliest, target.examined_root)))
        .collect()
}
