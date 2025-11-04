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

# ⚙️ EnerSync – SalzburgAG Smart Meter CSV Importer
## Configuration Reference

This section documents all configuration fields for the add‑on.  
Use it as a local guide if Home Assistant UI descriptions are not visible.

---

## 📁 File Handling

| Option | Description | Default |
|-------|-------------|--------|
`input_dir` | Folder where CSV files are placed for import | `/share/enersync/input`
`processed_dir` | Folder where successfully imported CSVs are moved | `/share/enersync/processed`
`error_dir` | Folder where failed CSVs are moved for inspection | `/share/enersync/error`
`csv_glob` | File pattern to match CSVs | `*.csv`
`scan_interval_seconds` | How often to check the input folder for new files | `60`
`batch_size` | Number of rows sent to HA per batch (improves performance) | `1000`

> ✅ Place CSVs into `/share/enersync/input`

---

## 📄 CSV Format

| Option | Description | Example |
|-------|-------------|--------|
`csv_encoding` | File encoding (auto or manual) | `utf-8`, `latin1`, `auto`
`csv_sep` | Separator | `;`, `,`, `auto`
`csv_decimal` | Decimal separator | `.`, `,`, `auto`
`csv_dayfirst` | Enable for `DD.MM.YYYY` dates | `true/false`
`csv_datetime_col` | Timestamp column name | autodetect if blank
`csv_value_col` | Energy value column name | autodetect if blank
`csv_status_col` | Optional status/quality column | e.g., `Status`
`csv_valid_status_regex` | Import only rows matching regex | `(?i)wert ist gültig`

> ⚠️ Leave timestamp/value empty unless your CSV differs from SalzburgAG format.

---

## 🕒 Time & Deduplication

| Option | Description | Values |
|-------|-------------|--------|
`timezone` | Local timezone | e.g. `Europe/Vienna`
`dedup` | When duplicate timestamps exist | `sum`, `first`, `mean`

> ✅ Recommended for utilities: `sum`

---

## 🔧 Calibration (Recommended)

| Option | Description | Example |
|--------|------------|--------|
`anchor_kwh` | Real meter reading | `7536.55`
`anchor_datetime` | Timestamp of reading | `30.06.2025 00:00`

> Aligns CSV cumulative sums to your **actual meter**.

---

## 🏠 Home Assistant

| Option | Description | Example |
|-------|-------------|--------|
`statistic_id` | Statistic entity to write | `sensor.stromverbrauch_salzburgAG`
`name` | Display name in HA | `SalzburgAG Import`
`ha_base_url` | Internal HA URL | `http://homeassistant:8123`
`ha_token` | HA **Long‑Lived Access Token** | *(generate in profile)*

> 🔐 Token stays local — never uploaded.

---

## ⏮ Import Control

| Option | Description | Notes |
|--------|-------------|------|
`reset_watermark` | Re‑import all data from first timestamp | Set `true` once then back to `false`

---

## 🧠 How It Works

- Watches input folder for CSVs  
- Parses + cleans energy values  
- Aligns cumulative totals to real meter (optional)  
- Sends to Home Assistant Recorder  
- Moves files to processed/error folders  
- Remembers last imported timestamp  

Supports **historical import + continuous updates**.

---

## 🏁 Quick Start

1. Generate HA Long‑Lived Token  
2. Enter token + statistic name  
3. Drop CSV inside `/share/enersync/input/`  
4. Open **Add‑on Logs**  
5. See ✅ `Imported`  

Then add the entity to **Energy Dashboard**.

---

## 📦 Recommended Settings

| Use Case | Setting |
|--------|--------|
European CSV | `csv_dayfirst: true`, `csv_decimal: auto`
Salzburg Netz | leave autodetect (recommended)
Avoid repeated rows | `dedup: sum`
Reuse oldest value first | `dedup: first`

---

## 🚀 Next Features (planned)

- Automatic Salzburg Netz login & download  
- GUI uploader  
- Export to InfluxDB / Prometheus  
- Water / Gas meters support  

---

Happy importing ⚡🇦🇹

Till Cortiel
