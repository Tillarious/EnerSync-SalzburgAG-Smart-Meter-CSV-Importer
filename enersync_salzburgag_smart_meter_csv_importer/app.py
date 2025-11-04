import os, time, json, re, shutil, traceback
from pathlib import Path
from datetime import datetime
import pandas as pd
from dateutil import tz
import requests
import websocket  # requires py3-websocket-client in the image

OPTIONS_PATH = Path("/data/options.json")
STATE_PATH = Path("/data/salznetze_state.json")

def log(msg: str): print(msg, flush=True)

def load_options():
    with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# -------------------- REST (fallback only) --------------------
def ha_call_service(path, data):
    """Fallback to legacy REST service, which expects metadata as a LIST."""
    opts = load_options()
    base = opts["ha_base_url"].rstrip("/")
    token = opts["ha_token"]
    if not token:
        raise RuntimeError("ha_token is empty — create a Long-Lived Access Token")
    url = base + path
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=data, timeout=120)
    if r.status_code not in (200, 201):
        try: err = r.json()
        except Exception: err = r.text
        raise RuntimeError(f"REST call FAILED {r.status_code}: {err}")

# -------------------- WebSocket (preferred on current HA) --------------------
def ws_connect():
    opts = load_options()
    base = opts["ha_base_url"].rstrip("/")
    token = opts["ha_token"]

    if base.startswith("http://"):
        ws_url = base.replace("http://", "ws://") + "/api/websocket"
    elif base.startswith("https://"):
        ws_url = base.replace("https://", "wss://") + "/api/websocket"
    else:
        ws_url = "ws://" + base + "/api/websocket"

    ws = websocket.create_connection(ws_url, timeout=30)
    hello = json.loads(ws.recv())
    if hello.get("type") != "auth_required":
        ws.close(); raise RuntimeError(f"WS unexpected hello: {hello}")
    ws.send(json.dumps({"type": "auth", "access_token": token}))
    auth = json.loads(ws.recv())
    if auth.get("type") != "auth_ok":
        ws.close(); raise RuntimeError(f"WS auth failed: {auth}")
    return ws

def ws_cmd(ws, payload):
    ws.send(json.dumps(payload))
    reply = json.loads(ws.recv())
    if not reply.get("success", False):
        raise RuntimeError(f"WS command failed: {reply}")
    return reply

def ws_import_statistics(metadata: dict, stats_list: list, chunk=1000):
    """
    Send recorder/import_statistics over WS in chunks.
    On your HA version, WS expects: metadata = dict, stats = list.
    """
    ws = ws_connect()
    try:
        total = len(stats_list)
        for i in range(0, total, chunk):
            part = stats_list[i:i+chunk]
            req = {
                "id": 1000 + i,
                "type": "recorder/import_statistics",
                "metadata": metadata,   # dict (NOT list) for WS
                "stats": part
            }
            ws_cmd(ws, req)
            log(f"→ WS imported {i+len(part)}/{total}")
    finally:
        try: ws.close()
        except: pass

# -------------------- CSV → stats --------------------
def _auto_sep(sample: str) -> str:
    cands = [",",";","\t","|"]
    counts = {c: sample.count(c) for c in cands}
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
    dt_col, val_col, st_col = detect_columns(
        df, opts.get("csv_datetime_col",""), opts.get("csv_value_col",""), opts.get("csv_status_col","")
    )

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
    if d.empty:
        raise RuntimeError("After filtering, no valid rows remain")

    zone = tz.gettz(opts["timezone"])
    ts_local = d["ts"].dt.tz_localize(zone, nonexistent="shift_forward", ambiguous="infer")
    ts_utc = ts_local.dt.tz_convert("UTC")

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
        df["sum_kwh"] = df["sum_csv_kwh"]; log("  calibration: none"); return df

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

def build_stats(df: pd.DataFrame) -> list:
    # Minimal rows: start + sum (UTC with Z suffix). Sum must be non-decreasing.
    return [
        {"start": row["ts_utc"].strftime("%Y-%m-%dT%H:%M:%SZ"),
         "sum": round(float(row["sum_kwh"]), 6)}
        for _, row in df.iterrows()
    ]

def load_state():
    if STATE_PATH.exists():
        try: return json.loads(STATE_PATH.read_text())
        except Exception: return {}
    return {}

def save_state(state: dict):
    STATE_PATH.write_text(json.dumps(state, indent=2))

def build_metadata(opts, source_value: str):
    return {
        "statistic_id": opts["statistic_id"],
        "unit_of_measurement": "kWh",
        "name": opts["name"],
        "source": source_value,     # must be a HA-accepted label
        "has_mean": False,
        "has_sum": True
    }

def process_csv(csv_path: Path, opts: dict):
    log(f"Processing: {csv_path.name}")
    d = parse_csv_to_df(csv_path, opts)
    d = apply_anchor(d, opts)

    # watermark
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
        log("  no new data — skipping"); return "skipped"

    stats = build_stats(d)

    # ---------- Probe for an accepted 'source' value ----------
    candidate_sources = ["recorder", "integration", "api", "external", "custom"]
    last_err = None
    accepted_metadata = None

    for src in candidate_sources:
        meta_try = build_metadata(opts, src)
        log(f"  DEBUG WS test payload (source='{src}'): {json.dumps({'metadata': meta_try, 'stats': stats[:1]}, ensure_ascii=False)}")
        try:
            ws = ws_connect()
            try:
                ws_cmd(ws, {
                    "id": 1,
                    "type": "recorder/import_statistics",
                    "metadata": meta_try,   # dict for WS
                    "stats": stats[:1]
                })
                log(f"✅ WS accepted 1-row test with source='{src}'")
                accepted_metadata = meta_try
                break
            finally:
                try: ws.close()
                except: pass
        except Exception as e:
            last_err = e
            msg = str(e)
            if "Invalid source" in msg or "invalid_format" in msg or "home_assistant_error" in msg:
                log(f"  ⚠ WS rejected source='{src}', trying next...")
                continue
            else:
                log(f"  ⚠ WS test failed for other reason: {e}")
                break

    if accepted_metadata is None:
        # Try REST fallback (uses list for metadata) with first candidate
        src = candidate_sources[0]
        meta_try = build_metadata(opts, src)
        try:
            ha_call_service("/api/services/recorder/import_statistics",
                            {"metadata": [meta_try], "stats": stats[:1]})
            log(f"✅ REST accepted 1-row test with source='{src}'")
            accepted_metadata = meta_try
        except Exception as e2:
            raise RuntimeError(f"No accepted source for metadata; last WS error: {last_err}; REST also failed: {e2}")

    # ---------- Bulk import ----------
    try:
        ws_import_statistics(accepted_metadata, stats, int(opts.get("batch_size", 1000)))
    except Exception as e:
        log(f"⚠ WS bulk failed: {e}")
        log("  Falling back to REST bulk...")
        total = len(stats); bs = int(opts.get("batch_size", 1000))
        for i in range(0, total, bs):
            part = stats[i:i+bs]
            ha_call_service("/api/services/recorder/import_statistics",
                            {"metadata": [accepted_metadata], "stats": part})
            log(f"→ REST imported {i+len(part)}/{total}")

    # watermark update
    new_last = d["ts_utc"].max().isoformat()
    state["last_ts_utc"] = new_last; save_state(state)
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
                    log(traceback.print_exc())
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
