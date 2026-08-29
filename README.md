# Fault-Tolerant Data Processing System

🔗 **Live demo:** https://fault-tolerant-data-processing-system-dli5.onrender.com

Flask + SQLite backend that takes in messy event data from clients, cleans
it up, makes sure the same event never gets counted twice, and gives back
aggregated numbers through an API. There's also a UI to test everything
manually.

---

## Run it locally

```bash
pip install -r requirements.txt
python app.py
```

Then open `http://localhost:5050`.

---

## What assumptions did I make?

**On unique IDs:**
Most clients won't send one. But if they do — `event_id`, `id`,
`idempotency_key`, `request_id`, or `uuid` — I trust it directly instead
of guessing.

**On retries without an ID:**
I treat a resent event as an exact retry of the same thing: same source,
same metric, same amount, same timestamp after normalizing. This matches
the brief's scenario of a client retrying a request.

The trade-off — two genuinely different events that happen to normalize
to identical values would collide and get treated as one. I accepted
this because under-counting a rare edge case felt safer than
double-counting every retry.

**On bad or missing data:**
- Missing/unparseable `amount` → defaults to `0`, flagged with a warning
  instead of being thrown away.
- Missing `timestamp` → falls back to whenever the event was received.
- Missing `source` → this is the *only* hard rejection, since without it
  there's no way to attribute or deduplicate the event.

**On the "simulate failure" toggle:**
It mimics a crash right after the raw event is logged, but before it's
written to the final table — the most realistic point for something to
actually break mid-request.

---

## How does the system prevent double counting?

**Two layers working together:**

1. Every event gets a **dedup hash** — either from a client-given ID, or
   a hash of `source + metric + amount + timestamp`.
2. Both tables (`raw_events`, `normalized_events`) have a `UNIQUE`
   constraint on that hash. Even if the app logic slipped up, SQLite
   itself won't insert a second row with the same hash.

**On ingest:** if a hash already exists and is marked `processed`, I just
return `"duplicate"` — `normalized_events` never gets touched again.

Since aggregation only reads from `normalized_events`, anything that
never fully processed simply isn't there to count. No separate dedup
logic to keep in sync on the aggregation side.

---

## What happens if the database write fails mid-request?

The write happens in **two separate steps**, not one atomic blob:

| Step | What happens |
|---|---|
| 1 | Insert into `raw_events`, status = `received`. This is the write-ahead log — once saved, the event survives any crash after this point. |
| 2 | Normalize the data → insert into `normalized_events` → flip `raw_events` status to `processed`. |

If step 2 fails (real DB error or the simulated toggle), the raw row is
marked `failed_write` with the error attached, and nothing lands in
`normalized_events` — so it's not counted yet.

When the client retries with the same payload, the dedup hash matches
the existing row. Since it's still `received`/`failed_write` (not
`processed`), the system **resumes** instead of creating a duplicate raw
row.

**Net result:** nothing lost, nothing double-counted. Worst case, an
event sits as `failed_write` until someone retries it — visible right in
the events table in the UI.

---

## What would break first at scale?

1. **SQLite** — single writer only. Fine for a demo, but concurrent
   clients writing at once would start locking up fast. First thing I'd
   swap for Postgres.

2. **Alias-based field matching** — works fine for a handful of clients,
   but as more get added, generic guessing (e.g. is `total` an amount or
   a count?) starts risking wrong mappings. `CLIENT_FIELD_ALIASES` is
   there so per-client overrides can be added without touching core
   logic, but at real scale this probably needs an explicit schema per
   client instead of inference.

3. **`/api/aggregate` computing live** — right now it's a `GROUP BY` over
   the whole table on every call. Fine at a few thousand rows; at
   millions with frequent polling, this needs pre-computed rollup tables
   instead.

4. **Everything running inside the request** — ingestion + normalization
   happen synchronously per HTTP request. Under real load this would
   benefit from a fast write-ahead log + a background worker doing the
   normalization — left out here since the brief said not to
   over-engineer the infrastructure.

---

## About the UI

- **Preset buttons** let you quickly try: a clean payload, a messy one
  (comma in amount, slashes in date), an unfamiliar client with
  different field names, and a broken one missing `source`.
- **"Simulate DB failure mid-write"** + submit → shows `failed_write`
  appear in the table. **"Retry same event"** → resolves it to
  `processed` without creating a second row.
- **"Processed / Failed Events"** tab = raw audit log of everything that
  hit the ingestion endpoint.
- **"Aggregated Results"** tab = only reflects what actually made it into
  the canonical store.
