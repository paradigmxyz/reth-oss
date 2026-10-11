//! Deadline and request-ownership regressions carry no consensus or availability authority.

use super::*;

struct Job {
    created: Instant,
    reply: oneshot::Sender<bool>,
}
impl StartupJob for Job {
    fn slot(&self) -> u64 {
        1
    }
    fn created(&self) -> Instant {
        self.created
    }
    fn closed(&self) -> bool {
        self.reply.is_closed()
    }
    fn unavailable(self) {
        let _ = self.reply.send(false);
    }
}
fn job() -> (Job, oneshot::Receiver<bool>) {
    let (reply, receiver) = oneshot::channel();
    (Job { created: Instant::now(), reply }, receiver)
}

#[test]
fn waiting_deadline_resolves_missing_before_receiver_timeout() {
    let mut broker = StartupRetrieval::<Job, u64>::default();
    let (original, mut receive) = job();
    broker.admit(original).unwrap();
    broker.waiting.as_mut().unwrap().started = Instant::now() - DATA_DEADLINE;
    broker.expire();
    assert!(!receive.try_recv().unwrap());
    assert!(!broker.has_work());
    assert!(!broker.has_request());
}

#[test]
fn expired_data_keeps_transport_token_and_does_not_consume_new_job() {
    let mut broker = StartupRetrieval::<Job, u64>::default();
    let (original, mut receive) = job();
    let peer = PeerId::random();
    broker.pending = Some(Pending {
        peer,
        id: 7,
        issued: Instant::now(),
        work: Some(Waiting {
            job: original,
            started: Instant::now() - DATA_DEADLINE,
            tried: vec![peer],
        }),
    });
    broker.expire();
    assert!(!receive.try_recv().unwrap());
    assert!(!broker.has_work());
    assert!(broker.has_request());
    let (next, mut next_receive) = job();
    broker.admit(next).unwrap();
    assert!(broker.has_work());
    assert!(broker.retire(peer, 8).is_none());
    assert!(broker.retire(PeerId::random(), 7).is_none());
    assert!(broker.has_request());
    assert!(matches!(broker.retire(peer, 7), Some(None)));
    assert!(!broker.has_request());
    assert!(broker.has_work());
    assert!(matches!(next_receive.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
    broker.waiting.as_mut().unwrap().started = Instant::now() - DATA_DEADLINE;
    broker.expire();
    assert!(!next_receive.try_recv().unwrap());
}

#[test]
fn alternate_peer_retry_preserves_original_deadline_and_admission() {
    let mut broker = StartupRetrieval::<Job, u64>::default();
    let (original, receive) = job();
    let peer = PeerId::random();
    let started = Instant::now() - Duration::from_secs(8);
    broker.pending = Some(Pending {
        peer,
        id: 7,
        issued: Instant::now(),
        work: Some(Waiting { job: original, started, tried: vec![peer] }),
    });
    assert!(!broker.unavailable(peer, 8).unwrap());
    assert!(broker.has_request());
    assert!(broker.unavailable(peer, 7).unwrap());
    assert!(!broker.has_request());
    let waiting = broker.waiting.as_ref().unwrap();
    assert_eq!(waiting.started, started);
    assert_eq!(waiting.tried, vec![peer]);
    let (duplicate, _duplicate_receive) = job();
    assert!(matches!(broker.admit(duplicate), Err(LiveSyncError::Bounds)));
    drop(receive);
    broker.expire();
    assert!(!broker.has_work());
}

#[test]
fn delayed_broker_admission_keeps_original_enqueue_deadline() {
    let mut broker = StartupRetrieval::<Job, u64>::default();
    let (mut original, mut receive) = job();
    original.created = Instant::now() - DATA_DEADLINE;
    broker.admit(original).unwrap();
    broker.expire();
    assert!(!receive.try_recv().unwrap());
    assert!(!broker.has_work());
    assert!(!broker.has_request());
}
