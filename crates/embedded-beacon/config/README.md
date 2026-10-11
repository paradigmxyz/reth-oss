# Beacon configuration

Hoodi network identity, genesis clock, announced historical activations, and blob parameter schedule are explicit. Gloas has no invented public activation; tests can add an explicit synthetic activation/version. All fork and digest lookups validate configuration and return errors for malformed schedules or zero timing parameters.

The current fork digest includes the blob-parameter-only hash mask beginning at Fulu. This network digest is separate from the signature domain, which continues to use ForkData. The implementation has exact reference-derived Hoodi digest regression values for genesis, Electra, Fulu, and the two announced blob schedule changes.

This module does not download or authenticate updated network metadata. Any future public Gloas activation must be supplied from verified network configuration.
## Explicit isolated devnet input

`DevnetConfig` parses at most 1 MiB of strict JSON, including nested fields. It requires schema version 1, an execution genesis hash, a terminal genesis block hash and activation epoch, and the `ChainConfig` consensus subset. Roots and fork versions use byte arrays; fork and preset names use the exact serialized enum names.

Private profiles require a nonpublic execution chain ID, the mainnet preset with 32 slots per epoch, every fork from Phase0 through a finite Gloas activation, bounded timing and resource limits, and terminal selection at the Bellatrix activation. Equal genesis activation epochs are supported. The launcher binds the input to the actual execution genesis and checks the paired execution activation timestamps.

The separate launcher accepts `--beacon.devnet-config PATH` only with `--embedded-beacon`. Genesis mode still requires `--beacon.genesis`; only an explicit checked private profile can omit a local history archive. This network subset does not replace a complete generated specification or verify the genesis state. Full state decoding, peer configuration, listeners, import coordination and recovery remain required, and the existing readiness gate refuses startup before opening the execution database.
