//! Local socket binding never changes advertised or outbound routing admission.

use super::*;

#[tokio::test]
async fn public_policy_can_open_an_actual_local_wildcard_listener_without_private_dials() {
    let mut listener = tests::transport(ChainConfig::hoodi(), 50700 * 32);
    listener.policy = EndpointPolicy::Public;
    listener.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let TransportEvent::Listening(address) = listener.next_event().await {
                assert!(address
                    .iter()
                    .any(|part| matches!(part, AddressPart::Tcp(port) if port != 0)));
                break
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        listener.dial(PeerId::random(), "/ip4/127.0.0.1/tcp/9000".parse().unwrap()),
        Err(TransportError::Address)
    );
    assert_eq!(
        listener.dial(PeerId::random(), "/ip4/0.0.0.0/tcp/9000".parse().unwrap()),
        Err(TransportError::Address)
    );
}

#[test]
fn bound_literals_do_not_allow_dns_relays_multicast_or_an_implicit_family_conversion() {
    for text in [
        "/dns/example.test/tcp/9000",
        "/ip4/224.0.0.1/tcp/9000",
        "/ip4/0.0.0.0/tcp/9000/p2p-circuit",
    ] {
        assert_eq!(
            validate_address(&text.parse().unwrap(), true, EndpointPolicy::Public),
            Err(TransportError::Address)
        );
    }
    assert!(validate_address(
        &"/ip6/::/udp/9001/quic-v1".parse().unwrap(),
        true,
        EndpointPolicy::Public
    )
    .is_ok());
    assert!(validate_address(
        &"/ip4/0.0.0.0/tcp/0".parse().unwrap(),
        true,
        EndpointPolicy::LoopbackOnly
    )
    .is_err());
}
