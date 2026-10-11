//! Artificial compliance fixtures only; never produces runtime validation authority.
#![cfg(feature = "spec-tests")]
use super::*;

fn upgrade_kernel(
    state: &mut BeaconState,
    parameters: &TransitionParameters,
) -> Result<(), ProcessingError> {
    validate_start(state, parameters)?;
    let epoch = state.slot() / parameters.slots_per_epoch();
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
                gloas_processing::spec_only::upgrade_to_gloas(value, &parameters.gloas)?,
                preset,
            ),
            BeaconState::Gloas(_, _) => {
                return Err(ProcessingError::InvalidState("cannot upgrade beyond Gloas"))
            }
        };
    }
    validate_start(state, parameters)
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
        BeaconState::Electra(value, _) => electra_processing::spec_only::process_slots(
            value,
            target,
            &parameters.gloas.fulu.electra,
        ),
        BeaconState::Fulu(value, _) => {
            fulu_processing::spec_only::process_slots(value, target, &parameters.gloas.fulu)
        }
        BeaconState::Gloas(value, _) => {
            gloas_processing::spec_only::process_slots(value, target, &parameters.gloas)
        }
    }
}

fn slots_kernel(
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
        advance_one_fork(state, end, parameters)?;
        upgrade_kernel(state, parameters)?;
    }
    validate_start(state, parameters)
}

/// Replay artificial compliance slots and trusted scheduled upgrades atomically.
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

/// Replay one artificial signed compliance block with exact trusted fork and post-root checks.
pub async fn state_transition<E: ExecutionEngine>(
    state: &mut BeaconState,
    block: &SignedBeaconBlock,
    parameters: &TransitionParameters,
    engine: &E,
) -> Result<(), ImportError<<E as bellatrix_processing::ExecutionEngine>::Error>> {
    validate_start(state, parameters)?;
    let header = block.header().map_err(ProcessingError::from)?;
    if block_preset(block) != parameters.preset() ||
        block.schema_fork() != parameters.fork_at_slot(header.slot)?
    {
        return Err(ProcessingError::InvalidState(
            "block schema or preset differs from trusted schedule",
        )
        .into())
    }
    let mut candidate = state.clone();
    if candidate.slot() < header.slot {
        slots_kernel(&mut candidate, header.slot, parameters)?;
    } else if candidate.slot() == header.slot {
        upgrade_kernel(&mut candidate, parameters)?;
    } else {
        return Err(ProcessingError::SlotNotIncreasing {
            current: candidate.slot(),
            target: header.slot,
        }
        .into())
    }
    match (&mut candidate, block) {
        (BeaconState::Phase0(value, _), SignedBeaconBlock::Phase0(message, _)) => {
            phase0_processing::spec_only::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0,
            )?;
        }
        (BeaconState::Altair(value, _), SignedBeaconBlock::Altair(message, _)) => {
            altair_processing::spec_only::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix.altair,
            )?;
        }
        (BeaconState::Bellatrix(value, _), SignedBeaconBlock::Bellatrix(message, _)) => {
            bellatrix_processing::spec_only::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella.bellatrix,
                engine,
            )
            .await?;
        }
        (BeaconState::Capella(value, _), SignedBeaconBlock::Capella(message, _)) => {
            capella_processing::spec_only::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb.capella,
                engine,
            )
            .await?;
        }
        (BeaconState::Deneb(value, _), SignedBeaconBlock::Deneb(message, _)) => {
            deneb_processing::spec_only::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra.deneb,
                engine,
            )
            .await?;
        }
        (BeaconState::Electra(value, _), SignedBeaconBlock::Electra(message, _)) => {
            electra_processing::spec_only::state_transition(
                value,
                message,
                &parameters.gloas.fulu.electra,
                engine,
            )
            .await?;
        }
        (BeaconState::Fulu(value, _), SignedBeaconBlock::Fulu(message, _)) => {
            fulu_processing::spec_only::state_transition(
                value,
                message,
                &parameters.gloas.fulu,
                engine,
            )
            .await?;
        }
        (BeaconState::Gloas(value, _), SignedBeaconBlock::Gloas(message, _)) => {
            gloas_processing::spec_only::state_transition(value, message, &parameters.gloas)?;
        }
        _ => {
            return Err(
                ProcessingError::InvalidState("block fork differs from processed state").into()
            )
        }
    }
    validate_start(&candidate, parameters)?;
    *state = candidate;
    Ok(())
}

#[cfg(test)]
mod tests;
