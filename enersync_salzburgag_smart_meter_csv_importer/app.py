import os, time, json, re, shutil, traceback
from pathlib import Path
from datetime import datetime
import pandas as pd
from dateutil import tz
import requests

OPTIONS_PATH = Path("/data/options.json")
STATE_PATH = Path("/data/salznetze_state.json")

def log(msg: str):
    print(msg, flush=True)

def load_options():
    with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# ✅ Updated for HA 2024.x
def ha_call(path, data):
    opts = load_options()
    base = opts["ha_base_url"].rstrip("/")
    token = opts["ha_token"]
    if not token:
        raise RuntimeError("ha_token empty — create Long-Lived Access Token")

    url = base + path
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    r = requests.post(url, headers=headers, json=data, timeout=120)
    if r.status_code not in (200, 201):
        try:
            detail = r.json()
        except:
            detail = r.text
        raise RuntimeError(f"HA API call FAILED {r.status_code}: {detail}")

def import_statistics(metadata: dict, stats: list, batch_size: int):
    total = len(stats)
    for i in range(0, total, batch_size):
        part = stats[i:i+batch_size]
        payload = {"metadata": [metadata], "stats": part}
        ha_call("/api/services/recorder/import_statistics", payload)
        log(f"→ Imported {i+len(part)}/{total}")

# Helpers
def _auto_sep(sample: str) -> str:
    candidates = [",", ";", "\t", "|"]
    counts = {c: sample.count(c) for c in candidates}
    return max(counts, key=counts.get) if any(counts.values()) else ","

def read_csv_flexible(csv_path: Path, enc: str, sep_opt: str, dec_opt: str) -> pd.DataFrame:
    raw = csv_path.read_text(encoding=enc, errors="ignore")
    sep = _auto_sep(raw) if sep_opt == "auto" else (sep_opt or ",")
    return pd.read_csv(csv_path, sep=sep, engine="python", encoding=enc)

def detect_columns(df, dt_override, val_override, st_override):
    if dt_override: dt = dt_override
    else:
        dt = next((c for c in df.columns if re.search(r"(datum|zeitpunkt|timestamp|time)", c, re.I)), None)

    if val_override: val = val_override
    else:
        val = next((c for c in df.columns if re.search(r"(kwh|wh|energie|verbrauch)", c, re.I)), None)

    st = st_override or next((c for c in df.columns if re.search(r"status|qualit", c, re.I)), None)

    if not dt or not val:
        raise RuntimeError(f"Can't detect timestamp/value columns — specify override")

    return dt, val, st

def parse_csv_to_df(csv_path: Path, opts: dict):
    log(f"  reading CSV: {csv_path}")

    df = read_csv_flexible(csv_path, opts["csv_encoding"], opts["csv_sep"], opts["csv_decimal"])
    dt_col, val_col, st_col = detect_columns(df, opts.get("csv_datetime_col",""), opts.get("csv_value_col",""), opts.get("csv_status_col",""))

    ts = pd.to_datetime(df[dt_col], dayfirst=bool(opts["csv_dayfirst"]), errors="coerce")

    s = df[val_col].astype(str)
    if opts["csv_decimal"] == "auto":
        if s.str.contains(",").sum() > s.str.contains(r"\.").sum():
            s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    elif opts["csv_decimal"] == ",":
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

    kwh = pd.to_numeric(s, errors="coerce")
    mask = ts.notna() & kwh.notna()

    d = pd.DataFrame({"ts": ts, "kwh": kwh})[mask].dropna()
    zone = tz.gettz(opts["timezone"])
    ts_local = d["ts"].dt.tz_localize(zone, nonexistent="shift_forward", ambiguous="infer")
    ts_utc = ts_local.dt.tz_convert("UTC")

    d = d.assign(ts_local=ts_local, ts_utc=ts_utc).sort_values("ts_utc")
    d = d.groupby("ts_local", as_index=False)["kwh"].sum()
    d = d.assign(ts_utc=d["ts_local"].dt.tz_convert("UTC")).sort_values("ts_utc")
    d["sum_csv_kwh"] = d["kwh"].cumsum()

    diffs = d["ts_utc"].diff().dropna().dt.total_seconds()
    off = int((diffs != 900).sum())
    if off:
        first_bad = d["ts_utc"].iloc[list((diffs != 900)[(diffs != 900)].index)[0]]
        log(f"  ⚠ found {off} non-15min intervals (first at {first_bad})")

    log(f"  points: {len(d)}, time range: {d['ts_utc'].min()} – {d['ts_utc'].max()}")
    return d

def apply_anchor(df, opts):
    anchor_kwh = opts.get("anchor_kwh")
    anchor_dt = opts.get("anchor_datetime")
    if not anchor_kwh or not anchor_dt:
        df["sum_kwh"] = df["sum_csv_kwh"]
        return df

    zone = tz.gettz(opts["timezone"])
    for fmt in ["%d.%m.%Y %H:%M", "%d.%m.%Y"]:
        try:
            dt = datetime.strptime(anchor_dt, fmt)
            break
        except: continue

    dt_local = dt.replace(tzinfo=zone)
    dt_utc = dt_local.astimezone(tz.gettz("UTC"))
    before = df[df["ts_utc"] <= dt_utc]

    offset = anchor_kwh if before.empty else (anchor_kwh - float(before["sum_csv_kwh"].iloc[-1]))
    df["sum_kwh"] = df["sum_csv_kwh"] + offset
    df.loc[df["sum_kwh"] < 0, "sum_kwh"] = 0
    return df

# ✅ HA expects Z timestamp, monotonic sum
def build_stats(df):
    return [
        {
            "start": row["ts_utc"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "state": round(float(row["sum_kwh"]), 6),
            "sum": round(float(row["sum_kwh"]), 6)
        }
        for _, row in df.iterrows()
    ]

def load_state():
    if STATE_PATH.exists():
        try: return json.loads(STATE_PATH.read_text())
        except: return {}
    return {}

def save_state(state): STATE_PATH.write_text(json.dumps(state, indent=2))

def process_csv(csv_path: Path, opts: dict):
    log(f"Processing: {csv_path.name}")
    d = parse_csv_to_df(csv_path, opts)
    d = apply_anchor(d, opts)

    state = load_state()
    if opts.get("reset_watermark"):
        state.pop("last_ts_utc", None)
        save_state(state)

    last = state.get("last_ts_utc")
    if last:
        last_dt = pd.to_datetime(last)
        d = d[d["ts_utc"] > last_dt]

    if d.empty:
        log("  no new data — skipping")
        return "skipped"

    stats = build_stats(d)

    metadata = {
        "statistic_id": opts["statistic_id"],
        "name": opts["name"],
        "source": "enersync_csv_addon",
        "unit_of_measurement": "kWh",
        "unit_class": "energy",
        "state_class": "total_increasing",
        "has_mean": False,
        "has_sum": True
    }

    # ✅ Test 1 point first
    test_payload = {"metadata": [metadata], "stats": stats[:1]}
    try:
        ha_call("/api/services/recorder/import_statistics", test_payload)
        log("✅ HA accepted 1-row test")
    except Exception as e:
        log(f"❌ HA rejected 1-row test: {e}")
        raise SystemExit("Stop — payload rejected by HA")

    import_statistics(metadata, stats, int(opts.get("batch_size", 1000)))

    new_last = d["ts_utc"].max().isoformat()
    state["last_ts_utc"] = new_last
    save_state(state)
    log(f"✅ updated watermark to {new_last}")
    return "imported"

def main():
    try:
        opts = load_options()
        input_dir = Path(opts["input_dir"])
        processed_dir = Path(opts["processed_dir"])
        error_dir = Path(opts["error_dir"])
        input_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)
        error_dir.mkdir(parents=True, exist_ok=True)

        log("📡 EnerSync CSV Importer started")
        log(f"Watching: {input_dir} (pattern: {opts['csv_glob']})")

        while True:
            for csv_path in sorted(input_dir.glob(opts["csv_glob"])):
                try:
                    status = process_csv(csv_path, opts)
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    shutil.move(str(csv_path), str(processed_dir / f"{csv_path.stem}_{ts}.csv"))
                    log(f"➡️ moved to processed ({status})")
                except Exception as e:
                    log(f"❌ error: {e}")
                    traceback.print_exc()
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    try:
                        shutil.move(str(csv_path), str(error_dir / f"{csv_path.stem}_{ts}.csv"))
                        log("➡️ moved to error folder")
                    except: log("❌ failed moving to error folder")
            time.sleep(int(opts.get("scan_interval_seconds", 60)))

    except Exception:
        traceback.print_exc()
        time.sleep(600)

if __name__ == "__main__":
    main()
