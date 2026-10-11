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

//! Actual stalled-consumer and publication-accounting regression controls.

use super::*;
use crate::{
    types::{Graft, Prune, RawMessage},
    TopicHash,
};
use futures::{executor::block_on, task::noop_waker};
use futures_timer::Delay;
use std::time::Duration;

fn control(index: usize) -> RpcOut {
    let topic = TopicHash::from_raw(format!("topic/{index}"));
    match index % 4 {
        0 => RpcOut::Subscribe(topic),
        1 => RpcOut::Graft(Graft { topic_hash: topic }),
        2 => RpcOut::Prune(Prune { topic_hash: topic, peers: Vec::new(), backoff: Some(60) }),
        _ => RpcOut::Unsubscribe(topic),
    }
}

fn publication(timeout: Delay) -> RpcOut {
    RpcOut::Publish {
        message: RawMessage {
            source: None,
            data: vec![1, 2, 3],
            sequence_number: None,
            topic: TopicHash::from_raw("publication"),
            signature: None,
            key: None,
            validated: true,
        },
        timeout,
    }
}

#[test]
fn stalled_priority_consumer_has_a_hard_bound_and_preserves_exact_control_fifo() {
    let sender = Sender::new(8);
    let mut receiver = sender.new_receiver();
    let capacity = MAX_PRIORITY_CONTROL_MESSAGES + 4;
    assert_eq!(sender.priority_sender.capacity(), Some(capacity));
    for index in 0..capacity {
        sender.send_message(control(index)).unwrap();
    }
    assert_eq!(sender.priority_sender.len(), capacity);
    let rejected = sender.send_message(control(capacity)).unwrap_err();
    assert_eq!(rejected.into_protobuf(), control(capacity).into_protobuf());
    assert_eq!(sender.priority_sender.len(), capacity);

    for index in 0..capacity {
        let original = block_on(receiver.next()).unwrap();
        assert_eq!(original.into_protobuf(), control(index).into_protobuf());
    }
    assert!(sender.priority_sender.is_empty());
    sender.send_message(control(capacity)).unwrap();
    assert_eq!(
        block_on(receiver.next()).unwrap().into_protobuf(),
        control(capacity).into_protobuf()
    );
}

#[test]
fn rejected_publication_releases_its_counter_when_controls_fill_the_priority_queue() {
    let sender = Sender::new(8);
    let mut receiver = sender.new_receiver();
    let capacity = MAX_PRIORITY_CONTROL_MESSAGES + 4;
    for index in 0..capacity {
        sender.send_message(control(index)).unwrap();
    }
    assert!(sender.send_message(publication(Delay::new(Duration::from_secs(60)))).is_err());
    assert_eq!(sender.len.load(Ordering::Relaxed), 0);
    assert_eq!(sender.priority_sender.len(), capacity);
    assert!(block_on(receiver.next()).is_some());
    sender.send_message(publication(Delay::new(Duration::from_secs(60)))).unwrap();
    assert_eq!(sender.len.load(Ordering::Relaxed), 1);
}

#[test]
fn stale_publication_releases_the_same_capacity_as_a_consumed_publication() {
    let sender = Sender::new(2);
    let mut receiver = sender.new_receiver();
    let mut expired = Delay::new(Duration::ZERO);
    block_on(&mut expired);
    sender.send_message(publication(expired)).unwrap();
    assert_eq!(sender.len.load(Ordering::Relaxed), 1);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(receiver.poll_stale(&mut cx), Poll::Ready(Some(RpcOut::Publish { .. }))));
    assert_eq!(sender.len.load(Ordering::Relaxed), 0);
    sender.send_message(publication(Delay::new(Duration::from_secs(60)))).unwrap();
    assert_eq!(sender.len.load(Ordering::Relaxed), 1);
}

#[test]
fn the_larger_control_queue_does_not_expand_the_existing_publication_limit() {
    let sender = Sender::new(4);
    let mut receiver = sender.new_receiver();
    for _ in 0..2 {
        sender.send_message(publication(Delay::new(Duration::from_secs(60)))).unwrap();
    }
    assert!(sender.send_message(publication(Delay::new(Duration::from_secs(60)))).is_err());
    assert_eq!(sender.len.load(Ordering::Relaxed), 2);
    assert!(matches!(block_on(receiver.next()), Some(RpcOut::Publish { .. })));
    assert_eq!(sender.len.load(Ordering::Relaxed), 1);
    sender.send_message(publication(Delay::new(Duration::from_secs(60)))).unwrap();
    assert_eq!(sender.len.load(Ordering::Relaxed), 2);
}
