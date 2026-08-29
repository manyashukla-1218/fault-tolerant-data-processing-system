"""
Fault-Tolerant Data Processing System
--------------------------------------
Single-file Flask backend. Kept intentionally simple (no docker,
no microservices, no auth) per the assignment constraints — the
focus is on the ingestion -> normalize -> dedup -> store -> aggregate
pipeline and how it survives partial failures & retries.
"""

import sqlite3
import json
import hashlib
import re
from datetime import datetime, timezone
from flask import Flask, request, jsonify, g, send_from_directory
from dateutil import parser as dateparser

DB_PATH = "data.db"
app = Flask(__name__, static_folder="static")

# ---------------------------------------------------------------------------
# DB SETUP
# ---------------------------------------------------------------------------

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(
        """
        -- Raw ledger: every request that hits the ingestion endpoint is
        -- logged here FIRST, keyed by a dedup hash. This is our
        -- write-ahead log / idempotency table. It is the source of truth
        -- for "have we ever seen this event before", independent of
        -- whether normalization/aggregation succeeded.
        CREATE TABLE IF NOT EXISTS raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dedup_hash TEXT UNIQUE NOT NULL,
            source TEXT,
            payload TEXT NOT NULL,
            received_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'received',
            error TEXT
        );

        -- Canonical, normalized events. This is what aggregation reads.
        -- dedup_hash is UNIQUE so even if something upstream misbehaves,
        -- the DB itself refuses to double-count.
        CREATE TABLE IF NOT EXISTS normalized_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dedup_hash TEXT UNIQUE NOT NULL,
            client_id TEXT NOT NULL,
            metric TEXT,
            amount REAL,
            timestamp TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (dedup_hash) REFERENCES raw_events(dedup_hash)
        );
        """
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# NORMALIZATION LAYER
# ---------------------------------------------------------------------------
# Design decision: normalization lives in its own module-like section,
# fully separate from ingestion (routes) and storage (DB). It is
# config-driven per client for KNOWN quirks, but falls back to a generic
# alias-based normalizer for unknown clients/fields so new clients don't
# require code changes (avoids "hardcode client-specific logic everywhere").

CLIENT_FIELD_ALIASES = {
    # Per-client overrides go here ONLY when a client's field names are
    # genuinely ambiguous under the generic alias matcher below.
    # Example: "client_B": {"amount": "amt_paid"}
}

GENERIC_ALIASES = {
    "metric": ["metric", "metric_name", "name", "type"],
    "amount": ["amount", "value", "amt", "total", "quantity"],
    "timestamp": ["timestamp", "time", "date", "ts", "created_at", "event_time"],
}


def _find_field(payload: dict, canonical_key: str, client_id: str):
    # 1. client-specific override
    override = CLIENT_FIELD_ALIASES.get(client_id, {}).get(canonical_key)
    if override and override in payload:
        return payload[override]
    # 2. generic alias list (case-insensitive EXACT match against payload keys)
    lower_map = {k.lower(): k for k in payload.keys()}
    for alias in GENERIC_ALIASES[canonical_key]:
        if alias in lower_map:
            return payload[lower_map[alias]]
    # 3. last resort: substring match (e.g. "amt_paid" contains "amt",
    # "event_time" contains "time"). Looser, so it's tried only after
    # exact matches fail — reduces false positives while still coping
    # with clients who add new/unexpected field name variants.
    for alias in GENERIC_ALIASES[canonical_key]:
        for lk, original_k in lower_map.items():
            if alias in lk:
                return payload[original_k]
    return None


def _coerce_amount(raw_amount):
    if raw_amount is None:
        return None, "amount field missing"
    if isinstance(raw_amount, (int, float)):
        return float(raw_amount), None
    if isinstance(raw_amount, str):
        cleaned = re.sub(r"[^0-9.\-]", "", raw_amount)
        if cleaned in ("", "-", "."):
            return None, f"amount '{raw_amount}' not numeric"
        try:
            return float(cleaned), None
        except ValueError:
            return None, f"amount '{raw_amount}' not numeric"
    return None, f"amount has unsupported type {type(raw_amount).__name__}"


def _coerce_timestamp(raw_ts):
    if raw_ts is None:
        # Missing timestamp is not fatal — we fall back to ingestion time,
        # but we flag it so it's visible in the UI/records.
        return datetime.now(timezone.utc).isoformat(), "timestamp missing, defaulted to now()"
    if isinstance(raw_ts, (int, float)):
        try:
            return datetime.fromtimestamp(raw_ts, tz=timezone.utc).isoformat(), None
        except (ValueError, OverflowError):
            return datetime.now(timezone.utc).isoformat(), f"timestamp '{raw_ts}' invalid epoch, defaulted"
    try:
        dt = dateparser.parse(str(raw_ts))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(), None
    except (ValueError, OverflowError, TypeError):
        return datetime.now(timezone.utc).isoformat(), f"timestamp '{raw_ts}' unparseable, defaulted to now()"


def normalize_event(source: str, payload: dict):
    """
    Converts a raw client payload into the canonical internal format.
    Returns (normalized_dict, warnings[]). Never raises for malformed
    data — malformed fields degrade gracefully with a recorded warning
    rather than rejecting the whole event, UNLESS a truly required field
    (client id / source) is absent, which is a hard validation failure.
    """
    warnings = []
    if not source:
        raise ValueError("missing required field: source/client id")

    metric = _find_field(payload, "metric", source)
    if metric is None:
        warnings.append("metric field missing")
        metric = "unknown"

    raw_amount = _find_field(payload, "amount", source)
    amount, amount_warning = _coerce_amount(raw_amount)
    if amount_warning:
        warnings.append(amount_warning)
        amount = 0.0  # graceful default so the event still counts as 'seen'

    raw_ts = _find_field(payload, "timestamp", source)
    timestamp, ts_warning = _coerce_timestamp(raw_ts)
    if ts_warning:
        warnings.append(ts_warning)

    normalized = {
        "client_id": source,
        "metric": str(metric),
        "amount": amount,
        "timestamp": timestamp,
    }
    return normalized, warnings


# ---------------------------------------------------------------------------
# IDEMPOTENCY / DEDUP KEY
# ---------------------------------------------------------------------------
# There is no guaranteed unique event ID from clients, so we use a layered
# strategy:
#   1. If the client happens to supply an explicit id-like field
#      (event_id / id / idempotency_key), trust it (namespaced by source).
#   2. Otherwise, derive a content hash from (source + normalized metric +
#      normalized amount + normalized timestamp). This is a deliberate
#      trade-off documented in the README: two *genuinely different*
#      events with identical source/metric/amount/timestamp would collide.
#      We accept that risk because it's the only stable signal we have,
#      and it exactly matches the retry scenario described in the brief
#      (same request resent verbatim).

ID_LIKE_FIELDS = ["event_id", "id", "idempotency_key", "request_id", "uuid"]


def compute_dedup_hash(source: str, raw_payload: dict, normalized: dict):
    for f in ID_LIKE_FIELDS:
        if f in raw_payload and raw_payload[f]:
            key = f"{source}:{raw_payload[f]}"
            return hashlib.sha256(key.encode()).hexdigest()
    key = f"{source}|{normalized['metric']}|{normalized['amount']}|{normalized['timestamp']}"
    return hashlib.sha256(key.encode()).hexdigest()


# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/ingest", methods=["POST"])
def ingest():
    """
    Ingestion pipeline, in order:
      1. Parse request body.
      2. Normalize (best-effort, never throws on bad data types).
      3. Compute dedup hash.
      4. Insert into raw_events (write-ahead log). If the hash already
         exists here, this is a retry/duplicate -> short-circuit safely.
      5. (optional) simulate a crash right here, AFTER the raw log is
         durable but BEFORE normalized_events is written, to prove the
         system can recover on retry without double counting or losing data.
      6. Insert into normalized_events and mark raw_events as processed.

    All of steps 4-6 run in a single SQLite transaction per step, and the
    two tables both carry a UNIQUE constraint on dedup_hash, so even a
    process crash between step 4 and step 6 leaves the system in a safely
    resumable state (see README: "what happens if the DB fails mid-request").
    """
    body = request.get_json(silent=True) or {}
    source = body.get("source")
    payload = body.get("payload", {})
    simulate_failure = bool(body.get("simulate_failure", False))

    db = get_db()

    # --- Step 2/3: normalize + hash (validation errors are hard failures) ---
    try:
        if not source:
            raise ValueError("missing required field: source")
        normalized, warnings = normalize_event(source, payload)
        dedup_hash = compute_dedup_hash(source, payload, normalized)
    except ValueError as e:
        # Hard validation failure — nothing durable is written for this
        # event other than a rejected-event audit trail. We still log a
        # best-effort row so the UI can show "rejected events".
        db.execute(
            "INSERT OR IGNORE INTO raw_events (dedup_hash, source, payload, received_at, status, error) "
            "VALUES (?, ?, ?, ?, 'rejected', ?)",
            (
                hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest(),
                source,
                json.dumps(body),
                datetime.now(timezone.utc).isoformat(),
                str(e),
            ),
        )
        db.commit()
        return jsonify({"status": "rejected", "reason": str(e)}), 422

    # --- Step 4: write-ahead log, guarded by UNIQUE(dedup_hash) ---
    existing = db.execute(
        "SELECT * FROM raw_events WHERE dedup_hash = ?", (dedup_hash,)
    ).fetchone()

    if existing is None:
        db.execute(
            "INSERT INTO raw_events (dedup_hash, source, payload, received_at, status) "
            "VALUES (?, ?, ?, ?, 'received')",
            (dedup_hash, source, json.dumps(body), datetime.now(timezone.utc).isoformat()),
        )
        db.commit()
    elif existing["status"] == "processed":
        # True duplicate/retry of an already-completed event.
        # Return success WITHOUT touching normalized_events again.
        return jsonify({
            "status": "duplicate",
            "message": "Event already processed previously; ignoring to avoid double counting.",
            "dedup_hash": dedup_hash,
        }), 200
    # else: existing row is 'received' or 'failed_write' -> a prior attempt
    # logged it but never finished. We fall through and RESUME processing
    # instead of re-inserting (avoids a second raw_events row for the
    # same logical event).

    # --- Step 5: failure injection point (for the UI's "simulate failure" toggle) ---
    if simulate_failure:
        db.execute(
            "UPDATE raw_events SET status = 'failed_write', error = ? WHERE dedup_hash = ?",
            ("Simulated DB failure after write-ahead log, before commit of normalized record", dedup_hash),
        )
        db.commit()
        return jsonify({
            "status": "error",
            "reason": "Simulated failure: DB write failed after event was durably logged. "
                      "Retry the same request (without simulate_failure) to resume safely.",
            "dedup_hash": dedup_hash,
        }), 500

    # --- Step 6: normalize write + mark processed ---
    try:
        db.execute(
            "INSERT OR IGNORE INTO normalized_events (dedup_hash, client_id, metric, amount, timestamp, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                dedup_hash,
                normalized["client_id"],
                normalized["metric"],
                normalized["amount"],
                normalized["timestamp"],
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        db.execute(
            "UPDATE raw_events SET status = 'processed', error = NULL WHERE dedup_hash = ?",
            (dedup_hash,),
        )
        db.commit()
    except sqlite3.Error as e:
        db.execute(
            "UPDATE raw_events SET status = 'failed_write', error = ? WHERE dedup_hash = ?",
            (str(e), dedup_hash),
        )
        db.commit()
        return jsonify({"status": "error", "reason": f"DB write failed: {e}", "dedup_hash": dedup_hash}), 500

    return jsonify({
        "status": "processed",
        "normalized": normalized,
        "warnings": warnings,
        "dedup_hash": dedup_hash,
    }), 200


@app.route("/api/events", methods=["GET"])
def list_events():
    """Returns raw ledger rows for the UI (processed / rejected / failed / received)."""
    status_filter = request.args.get("status")
    db = get_db()
    if status_filter:
        rows = db.execute(
            "SELECT * FROM raw_events WHERE status = ? ORDER BY id DESC LIMIT 100",
            (status_filter,),
        ).fetchall()
    else:
        rows = db.execute("SELECT * FROM raw_events ORDER BY id DESC LIMIT 100").fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/aggregate", methods=["GET"])
def aggregate():
    """
    Aggregation is deliberately read-only over normalized_events ONLY
    (never raw_events), so it's naturally consistent with dedup: a
    retried/duplicate event that never made it into normalized_events
    simply can't be double-counted here, by construction.
    """
    client = request.args.get("client")
    metric = request.args.get("metric")
    start = request.args.get("start")  # ISO date/time
    end = request.args.get("end")

    query = "SELECT client_id, metric, COUNT(*) as count, SUM(amount) as total, AVG(amount) as avg FROM normalized_events WHERE 1=1"
    params = []
    if client:
        query += " AND client_id = ?"
        params.append(client)
    if metric:
        query += " AND metric = ?"
        params.append(metric)
    if start:
        query += " AND timestamp >= ?"
        params.append(start)
    if end:
        query += " AND timestamp <= ?"
        params.append(end)
    query += " GROUP BY client_id, metric ORDER BY client_id, metric"

    db = get_db()
    rows = db.execute(query, params).fetchall()
    results = [dict(r) for r in rows]

    overall = db.execute(
        "SELECT COUNT(*) as count, SUM(amount) as total FROM normalized_events"
    ).fetchone()

    return jsonify({
        "filters": {"client": client, "metric": metric, "start": start, "end": end},
        "breakdown": results,
        "overall": dict(overall),
    })


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
