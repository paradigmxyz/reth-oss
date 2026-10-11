#!/usr/bin/env sh
set -eu
# Only the executable's concrete capability document can authorize participant launch.
jq -e 'type == "object" and .target_fork == "gloas" and
  .full_state_transition == true and .full_fork_choice == true and
  .peer_runtime == true and .checkpoint_bootstrap == true and
  .verified_state_recovery == true and .full_spec_conformance == true' "$1" > /dev/null
