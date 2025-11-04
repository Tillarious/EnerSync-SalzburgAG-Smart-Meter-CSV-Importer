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

# -------- Home Assistant REST call --------
def ha_call(path: str, data: dict):
    opts = load_options()
    base = opts["ha_base_url"].rstrip("/")
    token = opts["ha_token"]
    if not token:
        raise RuntimeError("ha_token is empty — create a Long-Lived Access Token and paste into add-on options.")
    url = base + path
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=data, timeout=120)
    if r.status_code not in (200, 201):
        # Try to surface JSON error, else text
        try:
            err = r.json()
        except Exception:
            err = r.text
        raise RuntimeError(f"HA API call FAILED {r.status_code}: {err}")

# -------- Import to HA recorder --------
def import_statistics(metadata: dict, stats: list, batch_size: int):
    """
    HA expects:
    {
      "metadata": [ { "statistic_id", "unit_of_measurement", "source", "name", "has_mean", "has_sum" } ],
      "stats":    [ { "start", "sum" }, ... ]
    }
    """
    total = len(stats)
    for i in range(0, total, batch_size):
        part = stats[i:i+batch_size]
        payload = {"metadata": [metadata], "stats": part}
        ha_call("/api/services/recorder/import_statistics", payload)
        log(f"→ Imported {i+len(part)}/{total}")

# -------- CSV helpers --------
def _auto_sep(sample: str) -> str:
    candidates = [",", ";", "\t", "|"]
    counts = {c: sample.count(c) for c in candidates}
    return max(counts, key=counts.get) if any(counts.values()) else ","

def read_csv_flexible(csv_path: Path, enc: str, sep_opt: str, dec_opt: str) -> pd.DataFrame:
    raw = csv_path.read_text(encoding=enc, errors="ignore")
    sep = _auto_sep(raw) if sep_opt == "auto" else (sep_opt or ",")
    return pd.read_csv(csv_path, sep=sep, engine="python", encoding=enc)

def detect_columns(df: pd.DataFrame, dt_override: str, val_override: str, st_override: str):
    if dt_override:
        dt = dt_override
    else:
        dt = next((c for c in df.columns if re.search(r"(datum|zeitpunkt|timestamp|time)", c, re.I)), None)

    if val_override:
        val = val_override
    else:
        val = next((c for c in df.columns if re.search(r"(kwh|wh|energie|verbrauch)", c, re.I)), None)

    st = st_override or next((c for c in df.columns if re.search(r"status|qualit", c, re.I)), None)

    if not dt or not val:
        raise RuntimeError(f"Unable to detect timestamp/value columns in {list(df.columns)}; set csv_datetime_col/csv_value_col.")
    return dt, val, st

def parse_csv_to_df(csv_path: Path, opts: dict) -> pd.DataFrame:
    log(f"  reading CSV: {csv_path}")
    df = read_csv_flexible(csv_path, opts["csv_encoding"], opts["csv_sep"], opts["csv_decimal"])
    dt_col, val_col, st_col = detect_columns(df, opts.get("csv_datetime_col",""), opts.get("csv_value_col",""), opts.get("csv_status_col",""))

    # parse timestamps
    ts = pd.to_datetime(df[dt_col], dayfirst=bool(opts["csv_dayfirst"]), errors="coerce")

    # parse kWh numbers with auto decimal heuristic
    s = df[val_col].astype(str)
    if opts["csv_decimal"] == "auto":
        if s.str.contains(",").sum() > s.str.contains(r"\.").sum():
            s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    elif opts["csv_decimal"] == ",":
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

    kwh = pd.to_numeric(s, errors="coerce")

    # filter valid rows
    mask = ts.notna() & kwh.notna()
    d = pd.DataFrame({"ts": ts, "kwh": kwh})[mask].dropna()
    if d.empty:
        raise RuntimeError("After filtering, no valid rows remain")

    # localize to local TZ, convert to UTC
    zone = tz.gettz(opts["timezone"])
    ts_local = d["ts"].dt.tz_localize(zone, nonexistent="shift_forward", ambiguous="infer")
    ts_utc = ts_local.dt.tz_convert("UTC")

    # dedup on local 15-min timestamp
    how = opts["dedup"] if opts["dedup"] in {"sum","first","mean"} else "sum"
    d = pd.DataFrame({"ts_local": ts_local, "ts_utc": ts_utc, "kwh": d["kwh"]})
    if how == "sum":
        d = d.groupby("ts_local", as_index=False)["kwh"].sum()
    elif how == "first":
        d = d.groupby("ts_local", as_index=False)["kwh"].first()
    else:
        d = d.groupby("ts_local", as_index=False)["kwh"].mean()

    d = d.assign(ts_utc=d["ts_local"].dt.tz_convert("UTC")).sort_values("ts_utc")
    d["sum_csv_kwh"] = d["kwh"].cumsum()

    # diagnostics
    diffs = d["ts_utc"].diff().dropna().dt.total_seconds()
    off = int((diffs != 900).sum())
    if off:
        first_bad = d["ts_utc"].iloc[list((diffs != 900)[(diffs != 900)].index)[0]]
        log(f"  ⚠ found {off} non-15min intervals (first at {first_bad})")
    log(f"  points: {len(d)}, time range: {d['ts_utc'].min()} – {d['ts_utc'].max()}")
    return d

def apply_anchor(df: pd.DataFrame, opts: dict) -> pd.DataFrame:
    anchor_kwh = opts.get("anchor_kwh")
    anchor_dt_local = opts.get("anchor_datetime")
    if not anchor_kwh or not anchor_dt_local:
        df["sum_kwh"] = df["sum_csv_kwh"]
        log("  calibration: none")
        return df

    zone = tz.gettz(opts["timezone"])
    parsed = None
    for fmt in ["%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y"]:
        try:
            parsed = datetime.strptime(anchor_dt_local, fmt)
            if fmt == "%d.%m.%Y":
                parsed = parsed.replace(hour=0, minute=0, second=0)
            break
        except ValueError:
            continue
    if parsed is None:
        raise RuntimeError("Could not parse anchor_datetime (use DD.MM.YYYY [HH:MM])")

    dt_local = parsed.replace(tzinfo=zone)
    dt_utc = dt_local.astimezone(tz.gettz("UTC"))
    before = df[df["ts_utc"] <= dt_utc]
    offset = anchor_kwh if before.empty else (anchor_kwh - float(before["sum_csv_kwh"].iloc[-1]))
    df["sum_kwh"] = df["sum_csv_kwh"] + offset
    df.loc[df["sum_kwh"] < 0, "sum_kwh"] = 0
    log(f"  calibration: anchor={anchor_kwh:.3f} kWh at {dt_local.strftime('%d.%m.%Y %H:%M %Z')}, offset={offset:.6f}")
    return df

# Build HA stats rows — send ONLY "start" + "sum"
def build_stats(df: pd.DataFrame) -> list:
    return [
        {
            "start": row["ts_utc"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sum": round(float(row["sum_kwh"]), 6)
        }
        for _, row in df.iterrows()
    ]

def load_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            return {}
    return {}

def save_state(state: dict):
    STATE_PATH.write_text(json.dumps(state, indent=2))

def process_csv(csv_path: Path, opts: dict):
    log(f"Processing: {csv_path.name}")
    d = parse_csv_to_df(csv_path, opts)
    d = apply_anchor(d, opts)

    # watermark
    state = load_state()
    if opts.get("reset_watermark"):
        state.pop("last_ts_utc", None)
        save_state(state)
        log("  watermark: reset requested")

    last_ts = state.get("last_ts_utc")
    if last_ts:
        last_dt = pd.to_datetime(last_ts)
        before = len(d)
        d = d[d["ts_utc"] > last_dt]
        log(f"  watermark: last={last_dt} → kept {len(d)}/{before}")

    if d.empty:
        log("  no new data — skipping")
        return "skipped"

    stats = build_stats(d)

    # Minimal, supported metadata ONLY
    metadata = {
        "statistic_id": opts["statistic_id"],
        "unit_of_measurement": "kWh",
        "source": "enersync_csv_addon",
        "name": opts["name"],
        "has_mean": False,
        "has_sum": True
    }

    # ---- Single-row test with full payload logging ----
    test_payload = {"metadata": [metadata], "stats": stats[:1]}
    log(f"  DEBUG test payload (1 row): {json.dumps(test_payload, ensure_ascii=False)}")
    try:
        ha_call("/api/services/recorder/import_statistics", test_payload)
        log("✅ HA accepted 1-row test")
    except Exception as e:
        log(f"❌ HA rejected 1-row test: {e}")
        raise SystemExit("Stop — payload rejected by HA")

    # Bulk import
    import_statistics(metadata, stats, int(opts.get("batch_size", 1000)))

    # update watermark
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
                    dest = processed_dir / f"{csv_path.stem}_{ts}.csv"
                    shutil.move(str(csv_path), str(dest))
                    log(f"➡️ moved to {dest} ({status})")
                except Exception as e:
                    log(f"❌ error processing {csv_path.name}: {e}")
                    log(traceback.format_exc())
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    dest = error_dir / f"{csv_path.stem}_{ts}.csv"
                    try:
                        shutil.move(str(csv_path), str(dest))
                        log(f"➡️ moved to {dest}")
                    except Exception as e2:
                        log(f"❌ move-to-error failed: {e2}")
            time.sleep(int(opts.get("scan_interval_seconds", 60)))
    except Exception:
        log(traceback.format_exc())
        time.sleep(600)

if __name__ == "__main__":
    main()
