
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
import json
import numpy as np

def _finite(a: np.ndarray) -> np.ndarray:
    arr = np.asarray(a, dtype=np.float32)
    return arr[np.isfinite(arr)]

def summarize_array(name: str, arr: np.ndarray, expected_min: float | None = None, expected_max: float | None = None) -> Dict[str, Any]:
    vals = _finite(arr)
    total = int(np.asarray(arr).size)
    finite = int(vals.size)
    out: Dict[str, Any] = {
        'name': name, 'count': total, 'finite_count': finite, 'nan_count': total - finite,
        'finite_fraction': float(finite / total) if total else 0.0,
        'min': None, 'max': None, 'mean': None, 'std': None,
        'p01': None, 'p05': None, 'p50': None, 'p95': None, 'p99': None,
        'expected_min': expected_min, 'expected_max': expected_max, 'out_of_expected_range': False,
    }
    if finite:
        out.update({
            'min': float(np.nanmin(vals)), 'max': float(np.nanmax(vals)), 'mean': float(np.nanmean(vals)), 'std': float(np.nanstd(vals)),
            'p01': float(np.nanpercentile(vals, 1)), 'p05': float(np.nanpercentile(vals, 5)), 'p50': float(np.nanpercentile(vals, 50)),
            'p95': float(np.nanpercentile(vals, 95)), 'p99': float(np.nanpercentile(vals, 99)),
        })
        if expected_min is not None and out['min'] < expected_min - 1e-6: out['out_of_expected_range'] = True
        if expected_max is not None and out['max'] > expected_max + 1e-6: out['out_of_expected_range'] = True
    return out

def build_time_sanity(time_id: str, species: str, layers: Dict[str, np.ndarray]) -> Dict[str, Any]:
    checks = {
        'front_boa_sst': (0.0, 1.0), 'front_boa_logchl': (0.0, 1.0), 'front_ssh': (0.0, 1.0),
        'front_persist_3d': (0.0, 1.0), 'front_persist_7d': (0.0, 1.0), 'front_fused': (0.0, 1.0),
        'vertical_access': (0.0, 1.0), 'ops_wind_penalty': (0.0, 1.0), 'phab': (0.0, 1.0), 'pops': (0.0, 1.0),
        'pcatch_scoring': (0.0, 1.0), 'pcatch_frontplus': (0.0, 1.0), 'pcatch_ensemble': (0.0, 1.0),
        'agree': (0.0, 1.0), 'spread': (0.0, 1.0), 'conf': (0.0, 1.0),
    }
    stats, flagged = {}, []
    for name, arr in layers.items():
        lo, hi = checks.get(name, (None, None))
        s = summarize_array(name, arr, lo, hi)
        stats[name] = s
        if s['out_of_expected_range']: flagged.append(name)
    return {'time_id': time_id, 'species': species, 'flagged_layers': flagged, 'layer_stats': stats}

def write_time_sanity(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

def build_species_sanity_summary(species: str, time_payloads: list[Dict[str, Any]]) -> Dict[str, Any]:
    flagged_counts: Dict[str, int] = {}
    finite_fractions: Dict[str, list[float]] = {}
    for p in time_payloads:
        for k in p.get('flagged_layers', []): flagged_counts[k] = flagged_counts.get(k, 0) + 1
        for name, s in p.get('layer_stats', {}).items(): finite_fractions.setdefault(name, []).append(float(s.get('finite_fraction', 0.0)))
    return {
        'species': species,
        'time_steps': len(time_payloads),
        'flagged_layer_counts': flagged_counts,
        'avg_finite_fraction': {k: float(np.mean(v)) for k, v in finite_fractions.items() if v},
    }
