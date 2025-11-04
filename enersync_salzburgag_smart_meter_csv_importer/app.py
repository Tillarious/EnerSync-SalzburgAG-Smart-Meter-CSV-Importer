import os, time, json, re, shutil, traceback
from pathlib import Path
from datetime import datetime
import pandas as pd
from dateutil import tz
import requests

OPTIONS_PATH = Path("/data/options.json")
STATE_PATH = Path("/data/salznetze_state.json")

def log(msg: str): print(msg, flush=True)

def load_options():
    with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def ha_call(path, data):
    opts = load_options()
    base = opts["ha_base_url"].rstrip("/")
    token = opts["ha_token"]
    if not token:
        raise RuntimeError("ha_token is empty. Create a Long-Lived Access Token in HA Profile and paste it into add-on options.")
    url = base + path
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=data, timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"HA API call failed {r.status_code}: {r.text}")

def import_statistics(metadata: dict, stats: list, batch_size: int):
    total = len(stats)
    for i in range(0, total, batch_size):
        part = stats[i:i+batch_size]
        ha_call("/api/services/recorder/import_statistics", {"metadata": metadata, "stats": part})
        log(f"→ Imported {i+len(part)}/{total}")

def _auto_sep(sample: str) -> str:
    # Guess delimiter by frequency
    candidates = [",", ";", "\t", "|"]
    counts = {c: sample.count(c) for c in candidates}
    return max(counts, key=counts.get) if any(counts.values()) else ","

def read_csv_flexible(csv_path: Path, enc: str, sep_opt: str, dec_opt: str) -> pd.DataFrame:
    raw = csv_path.read_text(encoding=enc, errors="ignore")
    sep = _auto_sep(raw) if sep_opt == "auto" else (sep_opt or ",")
    # Try parse quickly; pandas engine=python tolerates mixed delimiters
    return pd.read_csv(csv_path, sep=sep, engine="python", encoding=enc)

def detect_columns(df: pd.DataFrame, dt_override: str, val_override: str, st_override: str):
    if dt_override: dt = dt_override
    else:
        dt = next((c for c in df.columns if re.search(r"(datum|zeitpunkt|timestamp|time)", c, re.I)), None)
    if val_override: val = val_override
    else:
        val = next((c for c in df.columns if re.search(r"(restverbrauch|verbrauch).*(kwh|wh)|\b(kwh|wh)\b|energie", c, re.I)), None)
    st = st_override or next((c for c in df.columns if re.search(r"status|qualit", c, re.I)), None)
    if not dt or not val:
        raise RuntimeError(f"Unable to detect timestamp/value columns in {list(df.columns)}; set csv_datetime_col/csv_value_col.")
    return dt, val, st

def parse_csv_to_df(csv_path: Path, opts: dict):
    log(f"  reading CSV: {csv_path}")
    df = read_csv_flexible(csv_path, opts["csv_encoding"], opts["csv_sep"], opts["csv_decimal"])
    dt_col, val_col, st_col = detect_columns(df, opts.get("csv_datetime_col",""), opts.get("csv_value_col",""), opts.get("csv_status_col",""))

    # timestamp
    ts = pd.to_datetime(df[dt_col], dayfirst=bool(opts["csv_dayfirst"]), errors="coerce")

    # value (kWh)
    s = df[val_col].astype(str)
    if opts["csv_decimal"] == "auto":
        # naive guess: prefer comma if many commas and few dots
        if s.str.contains(",").sum() > s.str.contains(r"\.").sum():
            s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    elif opts["csv_decimal"] == ",":
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    # else keep "." as default
    kwh = pd.to_numeric(s, errors="coerce")

    # validity filter
    mask = ts.notna() & kwh.notna()
    regex = (opts.get("csv_valid_status_regex") or "").strip()
    if st_col and regex:
        st = df[st_col].astype(str)
        try:
            valid = st.str.contains(regex, na=False, regex=True)
            mask &= valid
        except re.error:
            log(f"  ⚠ invalid regex in csv_valid_status_regex: {regex} (ignoring)")

    d = pd.DataFrame({"ts": ts, "kwh": kwh})[mask].dropna()
    if d.empty: raise RuntimeError("After filtering, no valid rows remain")

    zone = tz.gettz(opts["timezone"])
    ts_local = d["ts"].dt.tz_localize(zone, nonexistent="shift_forward", ambiguous="infer")
    ts_utc = ts_local.dt.tz_convert("UTC")
    d = d.assign(ts_local=ts_local, ts_utc=ts_utc).sort_values("ts_utc")

    # deduplicate per local interval
    how = opts["dedup"] if opts["dedup"] in {"sum","first","mean"} else "sum"
    if how == "sum":   d = d.groupby("ts_local", as_index=False)["kwh"].sum()
    elif how == "first": d = d.groupby("ts_local", as_index=False)["kwh"].first()
    else:              d = d.groupby("ts_local", as_index=False)["kwh"].mean()

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

def apply_anchor(df: pd.DataFrame, opts: dict):
    anchor_kwh = opts.get("anchor_kwh")
    anchor_dt_local = opts.get("anchor_datetime")
    if not anchor_kwh or not anchor_dt_local:
        df["sum_kwh"] = df["sum_csv_kwh"]; log("  calibration: none (no anchor)"); return df

    zone = tz.gettz(opts["timezone"])
    dt = None
    for fmt in ["%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y"]:
        try:
            dt = datetime.strptime(anchor_dt_local, fmt)
            if fmt == "%d.%m.%Y": dt = dt.replace(hour=0, minute=0, second=0)
            break
        except ValueError: continue
    if dt is None: raise RuntimeError("Could not parse anchor_datetime (use DD.MM.YYYY [HH:MM])")

    dt_local = dt.replace(tzinfo=zone)
    dt_utc = dt_local.astimezone(tz.gettz("UTC"))
    df_before = df[df["ts_utc"] <= dt_utc]
    offset = anchor_kwh if df_before.empty else (anchor_kwh - float(df_before["sum_csv_kwh"].iloc[-1]))
    df["sum_kwh"] = df["sum_csv_kwh"] + offset
    mn = df["sum_kwh"].min()
    if mn < 0: df["sum_kwh"] = df["sum_kwh"] - mn
    log(f"  calibration: anchor={anchor_kwh:.3f} kWh at {dt_local.strftime('%d.%m.%Y %H:%M %Z')}, offset={offset:.6f}")
    return df

def build_stats(df: pd.DataFrame):
    return [
        {"start": row["ts_utc"].isoformat().replace("+00:00","Z"),
         "state": round(float(row["sum_kwh"]),6),
         "sum":   round(float(row["sum_kwh"]),6)}
        for _, row in df.iterrows()
    ]

def load_state():
    if STATE_PATH.exists():
        try: return json.loads(STATE_PATH.read_text())
        except Exception: return {}
    return {}

def save_state(state: dict):
    STATE_PATH.write_text(json.dumps(state, indent=2))

def process_csv(csv_path: Path, opts: dict):
    log(f"Processing: {csv_path.name}")
    d = parse_csv_to_df(csv_path, opts)
    d = apply_anchor(d, opts)

    state = load_state()
    if opts.get("reset_watermark"):
        state.pop("last_ts_utc", None); save_state(state); log("  watermark: reset requested")

    last_ts = state.get("last_ts_utc")
    if last_ts:
        last_dt = pd.to_datetime(last_ts)
        before = len(d)
        d = d[d["ts_utc"] > last_dt]
        log(f"  watermark: last={last_dt} → kept {len(d)}/{before}")

    if d.empty:
        log("  no new data after watermark — skipping")
        return "skipped"

    stats = build_stats(d)
    metadata = {
        "statistic_id": opts["statistic_id"],
        "unit_of_measurement": "kWh",
        "source": "salzburgnetze_csv_addon",
        "name": opts["name"]
    }
    import_statistics(metadata, stats, int(opts.get("batch_size", 1000)))
    new_last = d["ts_utc"].max().isoformat()
    state["last_ts_utc"] = new_last; save_state(state)
    log(f"  ✅ updated watermark to {new_last}")
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

        log("📡 SalzburgNetze CSV Importer started")
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
    except Exception as e:
        log(f"❌ FATAL: {e}")
        log(traceback.format_exc())
        time.sleep(600)

if __name__ == "__main__":
    main()
