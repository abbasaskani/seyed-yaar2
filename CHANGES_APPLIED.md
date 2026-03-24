
# Changes Applied

## Backend completed
- Added BOA front layers for SST and logCHL.
- Added SSH front and 3d/7d front persistence.
- Added eddy metrics: EKE, vorticity, strain, Okubo-Weiss, eddy-edge distance.
- Added vertical layers: MLD, O2, SSS and derived vertical_access.
- Added lagged productivity: CHL 3d/7d means, CHL anomaly, NPP anomaly.
- Added simple surface wind ops: wind_speed, wind_direction, ops_wind_penalty.
- Depth-aware datasets now resolve to the depth closest to surface; wind remains surface-only.
- Added per-time `sanity.json` and per-species `sanity_summary.json` outputs.
- Species meta now includes `species_profile`, `audit`, `defaults`, and sanity summary.

## Front-end completed
- Map selector now exposes advanced backend layers dynamically from `meta.paths.per_time`.
- Nonstandard layers can be selected directly without changing the UI layout.
- Profile and audit panels now show the added backend feature family and sanity summary.

## Still intentionally not done
- Scientific weight tuning / calibration from catches.
- New UI tabs or major UI redesign.
- Advanced Lagrangian / LAVD / material-coherence methods.
