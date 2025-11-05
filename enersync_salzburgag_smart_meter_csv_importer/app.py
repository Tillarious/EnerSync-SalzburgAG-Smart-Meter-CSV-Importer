import os, time, json, re, shutil, traceback, threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
from dateutil import tz
import requests
import websocket  # requires py3-websocket-client in the image
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
import cgi

OPTIONS_PATH = Path("/data/options.json")
STATE_PATH = Path("/data/salznetze_state.json")  # still used as a lightweight cache

def log(msg: str): print(msg, flush=True)

def load_options():
    with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

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

# -------------------- REST fallback (older HA only) --------------------
def ha_call_service(path, data):
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

# -------------------- HA statistics helpers --------------------
def get_last_stat_from_ha(statistic_id: str):
    """
    Returns (last_start_utc: datetime, last_sum: float) for the given statistic_id,
    or (None, None) if nothing exists yet. Uses WS recorder/get_statistics hour period.
    Tries recent 60 days first, then expands to 5 years if needed.
    """
    def query(period_days: int):
        ws = ws_connect()
        try:
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=period_days)
            req = {
                "id": 9001,
                "type": "recorder/get_statistics",
                "start_time": start.isoformat(),
                "end_time": end.isoformat(),
                "period": "hour",
                "statistic_ids": [statistic_id],
                "include_start_time": True
            }
            reply = ws_cmd(ws, req)
            result = reply.get("result") or {}
            series = result.get(statistic_id) or []
            if not series:
                return None
            # series is list of {"start": "...Z", "sum": float, ...}
            last = series[-1]
            last_start = datetime.fromisoformat(last["start"].replace("Z","+00:00"))
            return (last_start, float(last.get("sum", 0.0)))
        finally:
            try: ws.close()
            except: pass

    r = query(60)
    if r is not None:
        return r
    return query(365*5)  # fall back to long window

def import_statistics_ws(metadata: dict, stats_list: list, chunk=1000):
    """
    recorder/import_statistics via WS. On current HA, WS expects metadata=dict, stats=list.
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

# -------------------- CSV & calibration --------------------
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
        val = next((c for c in df.columns if re.search(r"(kwh|wh|energie|verbrauch|restverbrauch)", c, re.I)), None)

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
        if s.str.contains(",").sum() > s.str_contains(r"\.").sum():
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

    # dedup per local quarter-hour (sum duplicates by default)
    how = opts.get("dedup", "sum")
    d = pd.DataFrame({"ts_local": ts_local, "ts_utc": ts_utc, "kwh": d["kwh"]})
    if how == "first":
        d = d.groupby("ts_local", as_index=False)["kwh"].first()
    elif how == "mean":
        d = d.groupby("ts_local", as_index=False)["kwh"].mean()
    else:
        d = d.groupby("ts_local", as_index=False)["kwh"].sum()

    d = d.assign(ts_utc=d["ts_local"].dt.tz_convert("UTC")).sort_values("ts_utc")

    # Diagnostics
    diffs = d["ts_utc"].diff().dropna().dt.total_seconds()
    bad = (diffs != 900).sum()
    if bad:
        first_bad = d["ts_utc"].iloc[list((diffs != 900)[(diffs != 900)].index)[0]]
        log(f"  ⚠ found {bad} non-15min intervals (first at {first_bad})")

    return d  # columns: ts_local, ts_utc, kwh (15-min deltas)

def hourly_delta(df_15m: pd.DataFrame) -> pd.DataFrame:
    """Collapse 15-min deltas to hourly deltas (sum of the 4 quarters)."""
    work = df_15m.copy()
    work["hour_start"] = work["ts_utc"].dt.floor("H")
    hourly = work.groupby("hour_start", as_index=False)["kwh"].sum().sort_values("hour_start")
    return hourly  # columns: hour_start (UTC), kwh (hourly delta)

def apply_anchor_if_needed(hourly: pd.DataFrame, opts: dict) -> pd.DataFrame:
    """
    If HA has no baseline, apply anchor_kwh at anchor_datetime (local tz) to
    build a cumulative series that matches the anchor. Otherwise, the resume logic
    will start from HA's last sum and this function is not used.
    """
    anchor_kwh = opts.get("anchor_kwh")
    anchor_dt_local = opts.get("anchor_datetime")
    if not anchor_kwh or not anchor_dt_local:
        # No anchor => start from 0
        hourly["sum_kwh"] = hourly["kwh"].cumsum()
        return hourly

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

    # Anchor converted to UTC top-of-hour
    dt_local = parsed.replace(tzinfo=zone)
    anchor_hour_utc = dt_local.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # Build raw cumulative from CSV hourly deltas
    hourly = hourly.copy()
    hourly["cum_csv"] = hourly["kwh"].cumsum()

    # Find cumulative at/just before anchor hour
    before = hourly[hourly["hour_start"] <= anchor_hour_utc]
    if before.empty:
        # All data after anchor -> we just offset up so that first hour equals anchor if it's the anchor hour
        offset = anchor_kwh
    else:
        cum_at_anchor = float(before["cum_csv"].iloc[-1])
        offset = anchor_kwh - cum_at_anchor

    hourly["sum_kwh"] = hourly["cum_csv"] + offset
    # enforce monotonic non-decreasing (safety)
    hourly["sum_kwh"] = hourly["sum_kwh"].cummax()
    return hourly

def build_stats_from_cumulative(hourly_with_sum: pd.DataFrame) -> list:
    return [
        {
            "start": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),  # UTC top-of-hour
            "sum": round(float(val), 6)
        }
        for ts, val in zip(hourly_with_sum["hour_start"], hourly_with_sum["sum_kwh"])
    ]

# -------------------- State cache (optional) --------------------
def load_state():
    if STATE_PATH.exists():
        try: return json.loads(STATE_PATH.read_text())
        except Exception: return {}
    return {}

def save_state(state: dict):
    STATE_PATH.write_text(json.dumps(state, indent=2))

# -------------------- Metadata --------------------
def build_metadata(opts, source_value: str):
    return {
        "statistic_id": opts["statistic_id"],
        "unit_of_measurement": "kWh",
        "name": opts["name"],
        "source": source_value,     # must be accepted by HA
        "has_mean": False,
        "has_sum": True
    }

# -------------------- Uploader (no mounts) --------------------
class UploadHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        opts = load_options()
        input_dir = opts["input_dir"]
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>EnerSync CSV Upload</title></head>
<body style="font-family: system-ui; padding: 24px;">
  <h2>EnerSync CSV Upload</h2>
  <form method="POST" enctype="multipart/form-data">
    <p><input type="file" name="file" accept=".csv" required></p>
    <p><button type="submit">Upload</button></p>
  </form>
  <p>Target: <code>{input_dir}</code></p>
</body></html>"""
        self.send_response(200)
        self.send_header("Content-Type","text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def do_POST(self):
        opts = load_options()
        input_dir = Path(opts["input_dir"])
        input_dir.mkdir(parents=True, exist_ok=True)

        ctype, pdict = cgi.parse_header(self.headers.get("content-type"))
        if ctype != "multipart/form-data":
            self.send_error(400, "Expected multipart/form-data")
            return

        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                environ={'REQUEST_METHOD':'POST'},
                                keep_blank_values=True)

        if "file" not in form or not form["file"].filename:
            self.send_error(400, "No file uploaded")
            return
        fn = form["file"].filename
        data = form["file"].file.read()
        # Ensure unique name with timestamp
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", fn)
        target = input_dir / f"{Path(safe).stem}_{ts}.csv"
        with open(target, "wb") as f:
            f.write(data)
        log(f"📥 Uploaded CSV -> {target}")
        self.send_response(200)
        self.send_header("Content-Type","text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(f"Uploaded as {target.name}\n".encode("utf-8"))

def maybe_start_uploader():
    opts = load_options()
    if not opts.get("enable_uploader", False):
        return
    port = int(opts.get("uploader_port", 8099))
    server = HTTPServer(("0.0.0.0", port), UploadHandler)
    log(f"🌐 Uploader listening on :{port} (open http://<HA-IP>:{port}/)")
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

# -------------------- Main processing --------------------
def process_csv(csv_path: Path, opts: dict):
    log(f"Processing: {csv_path.name}")
    df_15 = parse_csv_to_df(csv_path, opts)
    hourly = hourly_delta(df_15)

    # Discover HA baseline (resume) — last stored hour + sum
    last = get_last_stat_from_ha(opts["statistic_id"])
    if last and all(v is not None for v in last):
        last_start_utc, last_sum = last
        log(f"  resume: HA last hour={last_start_utc} sum={last_sum}")
        # Drop any hours <= last_start_utc
        before = len(hourly)
        hourly = hourly[hourly["hour_start"] > last_start_utc]
        log(f"  resume: kept {len(hourly)}/{before} hours after HA baseline")
        # Build cumulative by adding deltas onto last_sum
        hourly = hourly.copy()
        hourly["sum_kwh"] = float(last_sum) + hourly["kwh"].cumsum()
        hourly["sum_kwh"] = hourly["sum_kwh"].cummax()
    else:
        log("  resume: no existing HA baseline, using anchor/zero")
        # No HA data -> use anchor if present, else start from 0
        hourly = apply_anchor_if_needed(hourly, opts)

    if hourly.empty:
        log("  no new hours to import — skipping")
        return "skipped"

    # Build final stats payload
    stats = build_stats_from_cumulative(hourly)

    # ---------- Determine accepted 'source' ----------
    candidate_sources = ["recorder", "integration", "api", "external", "custom"]
    accepted_metadata = None
    last_err = None
    for src in candidate_sources:
        meta_try = build_metadata(opts, src)
        test_payload = {"metadata": meta_try, "stats": stats[:1]}
        log(f"  DEBUG WS test payload (source='{src}'): {json.dumps(test_payload, ensure_ascii=False)}")
        try:
            ws = ws_connect()
            try:
                ws_cmd(ws, {
                    "id": 1,
                    "type": "recorder/import_statistics",
                    "metadata": meta_try,
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
        # Try REST fallback (metadata as list) with first candidate
        src = candidate_sources[0]
        meta_try = build_metadata(opts, src)
        try:
            ha_call_service("/api/services/recorder/import_statistics",
                            {"metadata": [meta_try], "stats": stats[:1]})
            log(f"✅ REST accepted 1-row test with source='{src}'")
            accepted_metadata = meta_try
        except Exception as e2:
            raise RuntimeError(f"No accepted source; WS last error: {last_err}; REST failed: {e2}")

    # ---------- Bulk import ----------
    try:
        import_statistics_ws(accepted_metadata, stats, int(opts.get("batch_size", 1000)))
    except Exception as e:
        log(f"⚠ WS bulk failed: {e}")
        log("  Falling back to REST bulk...")
        total = len(stats); bs = int(opts.get("batch_size", 1000))
        for i in range(0, total, bs):
            part = stats[i:i+bs]
            ha_call_service("/api/services/recorder/import_statistics",
                            {"metadata": [accepted_metadata], "stats": part})
            log(f"→ REST imported {i+len(part)}/{total}")

    # Update local cache watermark to the last raw 15-min point we processed
    state = load_state()
    new_last = df_15["ts_utc"].max().isoformat()
    state["last_ts_utc"] = new_last
    save_state(state)
    log(f"✅ updated watermark cache to {new_last}")
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

        maybe_start_uploader()

        log("📡 EnerSync CSV Importer (resume + anchor) started")
        log(f"Watching: {input_dir} (pattern: {opts.get('csv_glob','*.csv')})")

        while True:
            for csv_path in sorted(input_dir.glob(opts.get("csv_glob","*.csv"))):
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
