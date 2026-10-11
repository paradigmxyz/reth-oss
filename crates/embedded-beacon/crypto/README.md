# Beacon cryptography

Ethereum BLS signatures use the proof-of-possession ciphersuite, compressed-point decoding, infinity rejection, and subgroup checks through blst. Single signatures, nonempty aggregates, public key aggregation, and the Altair empty-participant infinity exception are implemented. ForkData, domain and SigningData roots use SSZ hashing.

Aggregate public keys must come from registries whose proofs of possession have already been established. Parsing a public key alone does not establish that proof. Crypto helpers never disable verification for normal node operation. Full bid/envelope eligibility and contextual signatures remain the state transition's responsibility.
