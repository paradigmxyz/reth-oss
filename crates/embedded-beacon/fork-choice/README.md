# Fork choice kernel

The validated-input ancestry kernel implements latest-message LMD-GHOST scores from justified-checkpoint weights, active/unslashed/equivocator exclusions, greatest-root tie breaking, voting-source leaf filtering, skipped-slot ancestry, proposer boost expiry, finality conflict checks, and execution-invalid subtree rejection. Invalid events validate before mutating the store. Invalidated messages are removed from weights.

The graph starts at a trusted execution-valid checkpoint, including a checkpoint block that precedes an empty epoch boundary. It never imports at or before the finalized epoch boundary. Invalidating a justified/finalized checkpoint requires explicit coordinator recovery and fails without changing the graph.

This kernel is not the complete fork-choice event handler. Inputs require upstream committee, BLS and full transition validation. Time-dependent checkpoint realization, historical checkpoint state retention, Gloas empty/full payload nodes, PTC quorum availability, proposer boost timing authentication, and complete fork-specific handlers remain outstanding. The node's full-transition gate prevents this kernel from accepting live unvalidated network data.


The production `Store` retains complete typed states behind private maps and imports Phase0 blocks through mandatory signature and post-state-root validation. Wire attestations use the actual target checkpoint state, shuffled committee and BLS verification; attester slashings authenticate both indexed attestations. Millisecond ticks realize pulled-up checkpoints at epoch boundaries and expire proposer boost. State snapshots share immutable allocations, bounded event checks reject resource excess, and failed events leave the store unchanged.

Published Minimal anchors from Altair through Gloas exercise root linkage and clock initialization. Two genuine published Phase0 signed blocks and their attestations exercise the production import path; their entire resulting state is compared with the original published post-state. Those regressions are not complete published fork-choice-suite runs. Later-fork event acceptance, execution optimism and Gloas payload/PTC handling remain explicitly unavailable. Structural anchors through Gloas do not imply those events are supported or that public sync is ready.
