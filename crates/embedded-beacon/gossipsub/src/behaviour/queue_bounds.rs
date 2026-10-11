// Copyright 2020 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! A mandatory control that cannot queue produces the real slow-peer failure.

use super::*;
use crate::config::ConfigBuilder;

#[test]
fn stalled_control_consumer_returns_failure_preserves_queue_and_emits_scored_slow_peer() {
    let config = ConfigBuilder::default()
        .validation_mode(ValidationMode::Anonymous)
        .connection_handler_queue_len(8)
        .build()
        .unwrap();
    let mut behaviour: Behaviour = Behaviour::new(MessageAuthenticity::Anonymous, config).unwrap();
    behaviour.with_peer_score(PeerScoreParams::default(), PeerScoreThresholds::default()).unwrap();
    let peer = PeerId::random();
    if let PeerScoreState::Active(score) = &mut behaviour.peer_score {
        score.add_peer(peer);
    } else {
        panic!("real scoring must stay enabled");
    }
    let sender = Sender::new(8);
    let capacity = sender.priority_sender.capacity().unwrap();
    assert_eq!(capacity, 4100);
    behaviour.connected_peers.insert(
        peer,
        PeerDetails {
            kind: PeerKind::Gossipsubv1_1,
            connections: vec![ConnectionId::new_unchecked(0)],
            outbound: false,
            topics: BTreeSet::new(),
            sender,
            dont_send: LinkedHashMap::new(),
        },
    );
    let score_before = behaviour.peer_score(&peer).unwrap();
    for index in 0..capacity {
        assert!(behaviour
            .send_message(peer, RpcOut::Subscribe(TopicHash::from_raw(format!("topic/{index}")))));
    }
    assert!(!behaviour.send_message(
        peer,
        RpcOut::Prune(Prune {
            topic_hash: TopicHash::from_raw("full"),
            peers: Vec::new(),
            backoff: Some(60),
        })
    ));
    assert_eq!(
        behaviour.connected_peers.get(&peer).unwrap().sender.priority_sender.len(),
        capacity
    );
    assert_eq!(behaviour.failed_messages.get(&peer).unwrap().priority, 1);
    assert!(behaviour.peer_score(&peer).unwrap() < score_before);

    behaviour.heartbeat();
    let failures = behaviour
        .events
        .iter()
        .find_map(|event| match event {
            ToSwarm::GenerateEvent(Event::SlowPeer { peer_id, failed_messages })
                if *peer_id == peer =>
            {
                Some(failed_messages)
            }
            _ => None,
        })
        .expect("undelivered mandatory control must reach the slow-peer owner");
    assert_eq!(failures.priority, 1);
    assert_eq!(failures.publish, 0);
    assert_eq!(failures.non_priority, 0);
}
