# Seyd-Yaar lean habitat-first patch

## What changed
- UI defaults switched to habitat-first mode.
- `pcatch` / `pops` removed from active UI flow.
- aggregation options reduced to `p90` and `mean`.
- time availability logic now falls back to habitat layers instead of failing on missing `pcatch` bins.
- async auto-analyze no longer throws uncaught promise errors.
- UI now handles empty `latest/` gracefully and ships with placeholder `latest/meta_index.json`.
- 12-hour timestamps are aligned to `06Z` and `18Z`.
- workflow defaults are lean: `skipjack`, `past=1`, `future=5`, `step=12`, `grid=160x160`.
- output root standardized to `latest/`.
- backend runtime default is habitat-first (`SEYDYAAR_ENABLE_OPS=0`).
- wave downloads are skipped unless ops are explicitly enabled.
- per-time writes are reduced to habitat-essential layers:
  - `phab_f32.bin`
  - `phab_frontplus_f32.bin`
  - `front_f32.bin`
  - `sst_f32.bin`
  - `chl_f32.bin`
  - `current_f32.bin`
  - `conf_f32.bin`
  - `qc_chl_u8.bin`

## Why this is faster
- no operational feasibility calculation by default
- no `pcatch` multiplication and no ensemble/agreement/spread writes
- no wave subset unless ops are re-enabled
- fewer binary writes per timestamp
- fewer UI fetches and simpler availability checks

## Re-enable ops later
Set:

```powershell
$env:SEYDYAAR_ENABLE_OPS="1"
```

or in GitHub Actions:

```yaml
SEYDYAAR_ENABLE_OPS: "1"
```
