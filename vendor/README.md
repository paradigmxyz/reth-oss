# Local frame-transaction adapters

These source snapshots keep the EIP-8250 devnet build on one Alloy and REVM revision while the
dependent adapter forks are updated. They are not new Reth APIs.

- `alloy-evm`: `Soubhik-10/evm` at `5ad553f277fcdf5d69e2cdf2ad20a11bb8ebfee1`; adds
  keyed transaction conversion, RPC simulation fields, and the nonce-manager fork transition.
- `reth-core`: `Soubhik-10/reth-core` at `bea4a854fea738bc8493989dfbad83298cd76636`;
  updates compact frame-transaction round trips to the keyed envelope.
- `revm-inspectors`: `Soubhik-10/revm-inspectors` at
  `4607ce46d4f95b017d2341a86997941f9b7ef678`; aligns trace/RPC dependencies.

Each snapshot retains its upstream MIT and Apache-2.0 license files. The three manifests pin
the EIP-8250 Alloy and REVM revisions used by this branch. Once equivalent commits are available
in the dependency forks, the path patches can be replaced by Git revisions.

This branch co-activates the keyed envelope with Bogota. Existing experimental frame-chain
databases encoded with the old single-`nonce` envelope cannot be replayed under this schema;
start a new devnet database. A later, separate EIP-8250 activation would require a fork-aware
dual decoder and historical compact codec before it can be supported.
