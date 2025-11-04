# EnerSync – SalzburgAG Smart Meter CSV Importer

Import SalzburgAG smart-meter CSV data into Home Assistant Energy statistics.

**How it works**
- Watches `/share/enersync/input` for `*.csv`
- Parses & groups 15‑min intervals, deduplicates, and builds cumulative kWh
- Optional calibration to a known meter value (anchor_kwh + anchor_datetime)
- Imports via HA REST API (`recorder.import_statistics`) using your Long‑Lived Access Token
- Watermark prevents duplicates; set `reset_watermark: true` once to re-import

**Setup**
1) Create a Long-Lived Access Token in HA (Profile → Create Token).
2) Install this add-on from your repo; open **Configuration**:
   - `ha_base_url`: `http://homeassistant:8123` (default)
   - `ha_token`: paste your token
   - (optional) `anchor_kwh`, `anchor_datetime`
3) Start add-on; it creates:
   - `/share/enersync/input`
   - `/share/enersync/processed`
   - `/share/enersync/error`
4) Drop CSVs into `/share/enersync/input`.

**Notes**
- Adjust parsing options if CSV format changes (`csv_sep`, `csv_decimal`, column overrides).
- Statistic is written to `statistic_id` (default: `sensor.energy_grid_import`).

MIT © Till Cortiel
