
# Ready to run

## Local backend run
```powershell
cd "C:\Users\MorBit\Documents\GitHub\seyed-yaar\backend"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cd ..
$env:PYTHONPATH=(Resolve-Path .ackend).Path
python -m seydyaar run-daily --out docs/latest --past-days 2 --future-days 8 --step-hours 6 --grid 220x220
```

## Local UI run
```powershell
cd "C:\Users\MorBit\Documents\GitHub\seyed-yaar"
python -m http.server 8081
```
Open:
- http://localhost:8081/
- http://localhost:8081/app.html

## What to verify after a run
- `docs/latest/meta_index.json`
- `docs/latest/runs/main/meta.json`
- `docs/latest/runs/main/variants/auto/species/<species>/meta.json`
- `docs/latest/runs/main/variants/auto/species/<species>/sanity_summary.json`
