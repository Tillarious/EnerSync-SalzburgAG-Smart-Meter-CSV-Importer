# EnerSync – SalzburgAG Smart Meter CSV Importer

Import SalzburgAG 15-minute smart-meter CSV data into Home Assistant Energy statistics.

![Logo](enersync_salzburgag_smart_meter_csv_importer/logo.png)

## Features
- CSV auto-detection (separator/decimal/date)
- 15-minute interval support
- Optional calibration to a real meter reading
- Safe incremental imports (watermark)
- Error and processed folders
- Works on HA OS without Supervisor tokens (uses Long-Lived Token)

## Install
1. Add this repo in **Add-on Store → Repositories**  
2. Install **EnerSync – SalzburgAG Smart Meter CSV Importer**  
3. Open **Configuration** and set:
   - `ha_base_url` (default ok)
   - `ha_token` (Long-Lived Token from your HA Profile)
   - Optional: `anchor_kwh` and `anchor_datetime`
4. Start the add-on. Drop CSVs into `/share/enersync/input`.
5. Use `sensor.stromverbrauch_salzburgAG` in the Energy Dashboard.
