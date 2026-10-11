//! Checked atomic slot replay and upgrades over a trusted eight-fork schedule.
//!
//! This dispatcher binds schema, preset, versions and fork epochs. It preserves exact old
//! fork epoch processing before an upgrade and applies consecutive equal-epoch upgrades in
//! order. Caller authentication and bounded slot/input resources remain protocol requirements.
//! Structural or fixture acceptance never enables full runtime readiness.

use crate::{
    altair_processing, bellatrix_processing, capella_processing, deneb_processing,
    electra_processing, fulu_processing, gloas_processing,
    phase0_processing::{self, ProcessingError},
    ssz::{BeaconState, Preset, SignedBeaconBlock},
};
use reth_embedded_beacon_primitives::Fork;

pub use crate::bellatrix_processing::ImportError;

mod gossip_replay;
pub use gossip_replay::replay_slots;

const FORKS: [Fork; 8] = [
    Fork::Phase0,
    Fork::Altair,
    Fork::Bellatrix,
    Fork::Capella,
    Fork::Deneb,
    Fork::Electra,
    Fork::Fulu,
    Fork::Gloas,
];

/// Trusted activation metadata for exactly one consensus fork.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForkActivation {
    /// Fork schema activated in this position of the ordered schedule.
    pub fork: Fork,
    /// Activation epoch; `u64::MAX` disables this and every following fork.
    pub epoch: u64,
    /// Trusted version used for state upgrades and signing domains.
    pub version: [u8; 4],
}
/// Complete inherited kernel parameters bound to an exact ordered schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransitionParameters {
    /// Nested parameters contain the exact clock, churn, BPO and signature rules for all forks.
    pub gloas: gloas_processing::GloasParameters,
    /// Exact eight-fork schedule; equal activation epochs are applied in specification order.
    pub schedule: Vec<ForkActivation>,
}
impl TransitionParameters {
    /// Bind inherited fork versions and epochs to a caller-authenticated schedule.
    pub fn new(
        gloas: gloas_processing::GloasParameters,
        schedule: Vec<ForkActivation>,
    ) -> Result<Self, ProcessingError> {
        if schedule.len() != 8 {
            return Err(ProcessingError::InvalidParameters(
                "fork schedule must contain exactly eight activations",
            ))
        }
        let mut parameters = Self { gloas, schedule };
        parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version =
            parameters.schedule[0].version;
        parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.fork_version =
            parameters.schedule[1].version;
        parameters.gloas.fulu.electra.deneb.capella.bellatrix.fork_version =
            parameters.schedule[2].version;
        parameters.gloas.fulu.electra.deneb.capella.fork_version = parameters.schedule[3].version;
        parameters.gloas.fulu.electra.deneb.fork_version = parameters.schedule[4].version;
        parameters.gloas.fulu.electra.fork_version = parameters.schedule[5].version;
        parameters.gloas.fulu.fork_version = parameters.schedule[6].version;
        parameters.gloas.fork_version = parameters.schedule[7].version;
        parameters.gloas.fork_epoch = parameters.schedule[7].epoch;
        parameters.gloas.fulu.fork_epoch = parameters.schedule[6].epoch;
        parameters.gloas.fulu.electra_fork_epoch = parameters.schedule[5].epoch;
        parameters.validate()?;
        Ok(parameters)
    }
    /// Exact pinned preset default schedule; speculative public activation remains disabled.
    pub fn for_preset(preset: Preset) -> Self {
        let gloas = gloas_processing::GloasParameters::for_preset(preset);
        let epochs = match preset {
            Preset::Mainnet => [0, 74240, 144896, 194048, 269568, 364032, 411392, u64::MAX],
            Preset::Minimal => {
                [0, u64::MAX, u64::MAX, u64::MAX, u64::MAX, u64::MAX, u64::MAX, u64::MAX]
            }
        };
        let schedule = FORKS
            .iter()
            .copied()
            .zip(epochs)
            .enumerate()
            .map(|(index, (fork, epoch))| ForkActivation {
                fork,
                epoch,
                version: [
                    index as u8,
                    0,
                    0,
                    match preset {
                        Preset::Mainnet => 0,
                        Preset::Minimal => 1,
                    },
                ],
            })
            .collect();
        Self { gloas, schedule }
    }
    /// Reject omitted/reordered forks, disabled gaps, duplicate versions or inconsistent kernels.
    pub fn validate(&self) -> Result<(), ProcessingError> {
        self.gloas.validate()?;
        if self.schedule.len() != FORKS.len() || self.schedule[0].epoch != 0 {
            return Err(ProcessingError::InvalidParameters(
                "fork schedule must start with Phase0 at genesis and contain eight forks",
            ))
        }
        let versions = [
            self.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            self.gloas.fulu.electra.deneb.capella.bellatrix.altair.fork_version,
            self.gloas.fulu.electra.deneb.capella.bellatrix.fork_version,
            self.gloas.fulu.electra.deneb.capella.fork_version,
            self.gloas.fulu.electra.deneb.fork_version,
            self.gloas.fulu.electra.fork_version,
            self.gloas.fulu.fork_version,
            self.gloas.fork_version,
        ];
        for (i, entry) in self.schedule.iter().enumerate() {
            if entry.fork != FORKS[i] ||
                entry.version != versions[i] ||
                (i > 0 && entry.epoch < self.schedule[i - 1].epoch) ||
                self.schedule[..i].iter().any(|prior| prior.version == entry.version)
            {
                return Err(ProcessingError::InvalidParameters(
                    "fork order, versions or epochs are inconsistent",
                ))
            }
            if entry.epoch != u64::MAX {
                entry
                    .epoch
                    .checked_mul(self.slots_per_epoch())
                    .ok_or(ProcessingError::InvalidParameters("fork activation slot overflows"))?;
            }
        }
        if self.gloas.fork_epoch != self.schedule[7].epoch ||
            self.gloas.fulu.fork_epoch != self.schedule[6].epoch ||
            self.gloas.fulu.electra_fork_epoch != self.schedule[5].epoch
        {
            return Err(ProcessingError::InvalidParameters(
                "fork-specific epoch bindings differ from schedule",
            ))
        }
        Ok(())
    }
    /// Exact SSZ/algorithm preset inherited by all scheduled forks.
    pub const fn preset(&self) -> Preset {
        self.gloas.preset()
    }
    /// Exact number of slots in one epoch under the selected preset.
    pub const fn slots_per_epoch(&self) -> u64 {
        self.gloas.slots_per_epoch()
    }
    /// Select the final enabled fork at this slot, including consecutive equal-epoch upgrades.
    pub fn fork_at_slot(&self, slot: u64) -> Result<Fork, ProcessingError> {
        self.validate()?;
        Ok(self
            .schedule
            .iter()
            .rev()
            .find(|entry| entry.epoch != u64::MAX && entry.epoch <= slot / self.slots_per_epoch())
            .ok_or(ProcessingError::InvalidParameters("schedule has no genesis fork"))?
            .fork)
    }
}
const fn state_preset(state: &BeaconState) -> Preset {
    match state {
        BeaconState::Phase0(_, preset) |
        BeaconState::Altair(_, preset) |
        BeaconState::Bellatrix(_, preset) |
        BeaconState::Capella(_, preset) |
        BeaconState::Deneb(_, preset) |
        BeaconState::Electra(_, preset) |
        BeaconState::Fulu(_, preset) |
        BeaconState::Gloas(_, preset) => *preset,
    }
}
fn validate_start(
    state: &BeaconState,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    parameters.validate()?;
    // This guard checks structure and schedule, not a committed root. Slot replay and the
    // signed block's post-state comparison still compute every required full commitment.
    state.validate_structure()?;
    if state_preset(state) != parameters.preset() {
        return Err(ProcessingError::InvalidState("state preset differs from trusted schedule"))
    }
    let own = parameters
        .schedule
        .iter()
        .position(|entry| entry.fork == state.schema_fork())
        .ok_or(ProcessingError::InvalidState("state schema is absent from schedule"))?;
    let activation = parameters.schedule[own];
    let info = state.fork_info();
    if activation.epoch == u64::MAX ||
        activation.epoch > state.slot() / parameters.slots_per_epoch() ||
        (info.current_version, info.epoch) != (activation.version, activation.epoch) ||
        (activation.epoch != 0 && info.previous_version != parameters.schedule[own - 1].version)
    {
        return Err(ProcessingError::InvalidState(
            "state fork versions or epoch differ from trusted schedule",
        ))
    }
    let effective = parameters.fork_at_slot(state.slot())?;
    if state.schema_fork() > effective {
        return Err(ProcessingError::InvalidState("state schema is ahead of trusted schedule"))
    }
    if state.schema_fork() < effective &&
        (!state.slot().is_multiple_of(parameters.slots_per_epoch()) ||
            parameters.schedule[own + 1].epoch != state.slot() / parameters.slots_per_epoch())
    {
        return Err(ProcessingError::InvalidState("state missed its scheduled fork boundary"))
    }
    Ok(())
}
fn upgrade_kernel(
    state: &mut BeaconState,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    validate_start(state, parameters)?;
    let epoch = state.slot() / parameters.slots_per_epoch();
    let mut upgraded = false;
    loop {
        let own = parameters
            .schedule
            .iter()
            .position(|entry| entry.fork == state.schema_fork())
            .ok_or(ProcessingError::InvalidState("unknown state schema"))?;
        if own == 7 ||
            parameters.schedule[own + 1].epoch == u64::MAX ||
            parameters.schedule[own + 1].epoch != epoch
        {
            break
        }
        if !state.slot().is_multiple_of(parameters.slots_per_epoch()) {
            return Err(ProcessingError::InvalidState("upgrade is not at an epoch boundary"))
        }
        let preset = parameters.preset();
        *state = match state {
            BeaconState::Phase0(value, _) => BeaconState::Altair(
                altair_processing::upgrade_to_altair(
                    value,
                    &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair,
                )?,
                preset,
            ),
            BeaconState::Altair(value, _) => BeaconState::Bellatrix(
                bellatrix_processing::upgrade_to_bellatrix(
                    value,
                    &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
                )?,
                preset,
            ),
            BeaconState::Bellatrix(value, _) => BeaconState::Capella(
                capella_processing::upgrade_to_capella(
                    value,
                    &parameters.gloas.fulu.electra.deneb.capella,
                )?,
                preset,
            ),
            BeaconState::Capella(value, _) => BeaconState::Deneb(
                deneb_processing::upgrade_to_deneb(value, &parameters.gloas.fulu.electra.deneb)?,
                preset,
            ),
            BeaconState::Deneb(value, _) => BeaconState::Electra(
                electra_processing::upgrade_to_electra(value, &parameters.gloas.fulu.electra)?,
                preset,
            ),
            BeaconState::Electra(value, _) => BeaconState::Fulu(
                fulu_processing::upgrade_to_fulu(value, &parameters.gloas.fulu)?,
                preset,
            ),
            BeaconState::Fulu(value, _) => BeaconState::Gloas(
                gloas_processing::upgrade_to_gloas(value, &parameters.gloas)?,
                preset,
            ),
            BeaconState::Gloas(_, _) => {
                return Err(ProcessingError::InvalidState("cannot upgrade beyond Gloas"))
            }
        };
        upgraded = true;
    }
    // An unchanged state was fully checked on entry; actual upgrades require a fresh check.
    if upgraded {
        validate_start(state, parameters)
    } else {
        Ok(())
    }
}
/// Apply all upgrades scheduled at this exact boundary atomically; other slots remain unchanged.
pub fn upgrade_state_at_slot(
    state: &mut BeaconState,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    let mut candidate = state.clone();
    upgrade_kernel(&mut candidate, parameters)?;
    *state = candidate;
    Ok(())
}
fn advance_one_fork(
    state: &mut BeaconState,
    target: u64,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    match state {
        BeaconState::Phase0(value, _) => phase0_processing::process_slots(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
        ),
        BeaconState::Altair(value, _) => altair_processing::process_slots(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair,
        ),
        BeaconState::Bellatrix(value, _) => bellatrix_processing::process_slots(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
        ),
        BeaconState::Capella(value, _) => capella_processing::process_slots(
            value,
            target,
            &parameters.gloas.fulu.electra.deneb.capella,
        ),
        BeaconState::Deneb(value, _) => {
            deneb_processing::process_slots(value, target, &parameters.gloas.fulu.electra.deneb)
        }
        BeaconState::Electra(value, _) => {
            electra_processing::process_slots(value, target, &parameters.gloas.fulu.electra)
        }
        BeaconState::Fulu(value, _) => {
            fulu_processing::process_slots(value, target, &parameters.gloas.fulu)
        }
        BeaconState::Gloas(value, _) => {
            gloas_processing::process_slots(value, target, &parameters.gloas)
        }
    }
}
fn slots_kernel(
    state: &mut BeaconState,
    target: u64,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    slots_kernel_with_replay::<false>(state, target, parameters)
}
fn slots_kernel_with_replay<const IN_PLACE: bool>(
    state: &mut BeaconState,
    target: u64,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    validate_start(state, parameters)?;
    if target <= state.slot() {
        return Err(ProcessingError::SlotNotIncreasing { current: state.slot(), target })
    }
    upgrade_kernel(state, parameters)?;
    while state.slot() < target {
        let boundary = parameters
            .schedule
            .iter()
            .find(|entry| entry.fork > state.schema_fork() && entry.epoch != u64::MAX)
            .map(|entry| {
                entry
                    .epoch
                    .checked_mul(parameters.slots_per_epoch())
                    .ok_or(ProcessingError::Arithmetic("fork boundary slot"))
            })
            .transpose()?;
        let end = boundary.map_or(target, |slot| slot.min(target));
        if end <= state.slot() {
            return Err(ProcessingError::InvalidState("unprocessed fork boundary"))
        }
        if IN_PLACE {
            gossip_replay::advance_one_fork_in_place(state, end, parameters)?;
        } else {
            advance_one_fork(state, end, parameters)?;
        }
        upgrade_kernel(state, parameters)?;
    }
    validate_start(state, parameters)
}
/// Atomically replay exact fork-aware empty slots, processing old epochs before scheduled upgrades.
pub fn process_slots(
    state: &mut BeaconState,
    target: u64,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    let mut candidate = state.clone();
    slots_kernel(&mut candidate, target, parameters)?;
    *state = candidate;
    Ok(())
}

/// One adapter bundle implementing every mandatory typed engine/terminal contract.
/// Execution authority remains inside each versioned callback; no verdict flag is accepted.
pub trait ExecutionEngine: bellatrix_processing::ExecutionEngine
    + capella_processing::ExecutionEngine<
        Error = <Self as bellatrix_processing::ExecutionEngine>::Error,
    > + deneb_processing::ExecutionEngine<
        Error = <Self as bellatrix_processing::ExecutionEngine>::Error,
    > + electra_processing::ExecutionEngine<
        Error = <Self as bellatrix_processing::ExecutionEngine>::Error,
    > + fulu_processing::ExecutionEngine<Error = <Self as bellatrix_processing::ExecutionEngine>::Error>
    + gloas_processing::ExecutionEngine<
        Error = <Self as bellatrix_processing::ExecutionEngine>::Error,
    >
{
}
impl<T> ExecutionEngine for T where
    T: bellatrix_processing::ExecutionEngine
        + capella_processing::ExecutionEngine<
            Error = <T as bellatrix_processing::ExecutionEngine>::Error,
        > + deneb_processing::ExecutionEngine<
            Error = <T as bellatrix_processing::ExecutionEngine>::Error,
        > + electra_processing::ExecutionEngine<
            Error = <T as bellatrix_processing::ExecutionEngine>::Error,
        > + fulu_processing::ExecutionEngine<
            Error = <T as bellatrix_processing::ExecutionEngine>::Error,
        > + gloas_processing::ExecutionEngine<
            Error = <T as bellatrix_processing::ExecutionEngine>::Error,
        >
{
}
const fn block_preset(block: &SignedBeaconBlock) -> Preset {
    match block {
        SignedBeaconBlock::Phase0(_, preset) |
        SignedBeaconBlock::Altair(_, preset) |
        SignedBeaconBlock::Bellatrix(_, preset) |
        SignedBeaconBlock::Capella(_, preset) |
        SignedBeaconBlock::Deneb(_, preset) |
        SignedBeaconBlock::Electra(_, preset) |
        SignedBeaconBlock::Fulu(_, preset) |
        SignedBeaconBlock::Gloas(_, preset) => *preset,
    }
}
/// Replay, upgrade and import one signed block atomically through its mandatory fork kernel.
/// Gloas beacon import remains separate from envelope verification and full-data admission.
pub async fn state_transition<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &TransitionParameters,
    engine: &E,
) -> Result<(), ImportError<<E as bellatrix_processing::ExecutionEngine>::Error>> {
    let block_slot = checked_signed_input(state, block, parameters)?;
    let candidate =
        state_transition_candidate(state.clone(), block, block_slot, parameters, engine).await?;
    *state = candidate;
    Ok(())
}

/// Consume one disposable complete state through the same mandatory signed dispatcher.
/// Errors and cancellation discard the owned copy; no partially changed state can be returned.
/// Schedule, schema, BLS, Engine callbacks and the complete post-state root remain mandatory.
pub async fn state_transition_owned<E: ExecutionEngine>(
    state: BeaconState,
    block: &SignedBeaconBlock,
    parameters: &TransitionParameters,
    engine: &E,
) -> Result<BeaconState, ImportError<<E as bellatrix_processing::ExecutionEngine>::Error>> {
    let block_slot = checked_signed_input(&state, block, parameters)?;
    state_transition_candidate(state, block, block_slot, parameters, engine).await
}

fn checked_signed_input(
    state: &BeaconState,
    block: &SignedBeaconBlock,
    parameters: &TransitionParameters,
) -> Result<u64, ProcessingError> {
    validate_start(state, parameters)?;
    let header = block.header().map_err(ProcessingError::from)?;
    if block_preset(block) != parameters.preset() ||
        block.schema_fork() != parameters.fork_at_slot(header.slot)?
    {
        return Err(ProcessingError::InvalidState(
            "block schema or preset differs from trusted schedule",
        ))
    }
    Ok(header.slot)
}

async fn state_transition_candidate<E: ExecutionEngine>(
    mut candidate: BeaconState,
    block: &SignedBeaconBlock,
    block_slot: u64,
    parameters: &TransitionParameters,
    engine: &E,
) -> Result<BeaconState, ImportError<<E as bellatrix_processing::ExecutionEngine>::Error>> {
    if candidate.slot() < block_slot {
        slots_kernel_with_replay::<true>(&mut candidate, block_slot, parameters)?;
    } else if candidate.slot() == block_slot {
        upgrade_kernel(&mut candidate, parameters)?;
    } else {
        return Err(ProcessingError::SlotNotIncreasing {
            current: candidate.slot(),
            target: block_slot,
        }
        .into())
    }
    match (&mut candidate, block) {
        (BeaconState::Phase0(value, _), SignedBeaconBlock::Phase0(message, _)) => {
            phase0_processing::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
            )?;
        }
        (BeaconState::Altair(value, _), SignedBeaconBlock::Altair(message, _)) => {
            altair_processing::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair,
            )?;
        }
        (BeaconState::Bellatrix(value, _), SignedBeaconBlock::Bellatrix(message, _)) => {
            bellatrix_processing::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
                engine,
            )
            .await?;
        }
        (BeaconState::Capella(value, _), SignedBeaconBlock::Capella(message, _)) => {
            capella_processing::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella,
                engine,
            )
            .await?;
        }
        (BeaconState::Deneb(value, _), SignedBeaconBlock::Deneb(message, _)) => {
            deneb_processing::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb,
                engine,
            )
            .await?;
        }
        (BeaconState::Electra(value, _), SignedBeaconBlock::Electra(message, _)) => {
            electra_processing::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra,
                engine,
            )
            .await?;
        }
        (BeaconState::Fulu(value, _), SignedBeaconBlock::Fulu(message, _)) => {
            fulu_processing::state_transition_in_place(
                value,
                message,
                &parameters.gloas.fulu,
                engine,
            )
            .await?;
        }
        (BeaconState::Gloas(value, _), SignedBeaconBlock::Gloas(message, _)) => {
            gloas_processing::state_transition_in_place(value, message, &parameters.gloas)?;
        }
        _ => {
            return Err(
                ProcessingError::InvalidState("block fork differs from processed state").into()
            )
        }
    }
    validate_start(&candidate, parameters)?;
    Ok(candidate)
}

#[cfg(test)]
mod tests;

/// Explicit artificial compliance replay; unavailable to runtime consumers.
#[cfg(feature = "spec-tests")]
pub mod spec_only;
