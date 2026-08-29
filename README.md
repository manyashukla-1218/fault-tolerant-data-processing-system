# Fault-Tolerant Data Processing System

Flask + SQLite backend that takes in messy event data from clients, cleans it up,
makes sure the same event never gets counted twice, and gives back aggregated
numbers through an API. There's also a UI to test everything manually.

## How to run it

```bash
pip install -r requirements.txt
python app.py
```

Then open `http://localhost:5050` in the browser.

## What assumptions did I make?

- Most clients won't send a proper unique ID with their events. But if they
  do send something like `event_id`, `id`, `idempotency_key`, `request_id`
  or `uuid`, I use that directly instead of trying to guess.
- If there's no ID, I'm treating a resent event as an exact retry of the
  same thing — meaning same source, same metric, same amount, same
  timestamp after normalizing. This matches what the assignment describes
  (client retries the request). The downside of this approach is that two
  actually different events could theoretically look identical and get
  treated as one — I decided that's a better trade-off than the other way
  around (double-counting every retry), since under-counting a rare edge
  case is safer than inflating numbers.
- If `amount` is missing or can't be parsed, I default it to 0 instead of
  throwing the whole event away, but I attach a warning so it's still
  visible in the logs. Same idea for missing timestamp — it just falls
  back to whenever the event was received.
- The only thing that actually gets rejected outright is a missing
  `source`/client id, because without that there's no way to know who
  sent it or to deduplicate it properly.
- The "simulate failure" checkbox in the UI mimics a crash that happens
  right after the raw event has been logged, but before it gets written
  into the final normalized table — since that's the most realistic place
  for something to actually go wrong mid-request.

## How does my system prevent double counting?

Two things work together here:

1. Every event gets a dedup hash — either from a client-given ID if one
   exists, or otherwise a hash made from `source + metric + amount +
   timestamp`.
2. Both database tables (`raw_events` and `normalized_events`) have a
   `UNIQUE` constraint on that hash. So even if I messed something up in
   the Python logic, SQLite itself won't allow a second row with the same
   hash to be inserted.

When a new request comes in, I check if a raw row with that hash already
exists and is marked `processed`. If yes, I just return `"duplicate"` and
don't touch `normalized_events` again. Since the aggregation endpoint only
ever reads from `normalized_events`, anything that never actually finished
processing simply isn't there to be counted — there's no separate dedup
logic I need to keep in sync on the aggregation side.

## What happens if the database write fails mid-request?

I split the write into two separate steps instead of doing it all at once:

1. First, insert into `raw_events` with status `received`. This is
   basically a write-ahead log — once this is saved, the event is never
   lost even if the server crashes right after.
2. Then normalize the data and insert into `normalized_events`, and only
   after that succeeds, flip the raw row's status to `processed`.

If step 2 fails (real DB error, or the simulated failure toggle), the raw
row gets marked `failed_write` along with the error message, and nothing
goes into `normalized_events` — so it doesn't get counted yet. When the
client retries with the exact same payload, the dedup hash matches the
existing row, sees it's still `received` or `failed_write` (not
`processed`), and just resumes from there instead of creating a duplicate
raw entry. End result: nothing gets lost, nothing gets counted twice, and
if it's stuck it just sits there as `failed_write` until someone retries
it — which you can see directly in the events table in the UI.

## What would break first at scale?

1. **SQLite.** It only allows one writer at a time, which is totally fine
   for a demo like this but would become a bottleneck fast with multiple
   clients hitting the API at once. This would be the first thing I'd
   swap for something like Postgres.
2. **The alias-based field matching.** Right now normalization guesses
   field names using a generic alias list plus substring matching, which
   works fine for a handful of clients. But as more clients get added,
   this guessing could start mapping the wrong field (e.g. one client's
   `total` might mean total amount, another's might mean total count).
   I left `CLIENT_FIELD_ALIASES` in place specifically so per-client
   overrides can be added without touching the core logic, but at real
   scale this probably needs to become an explicit schema per client
   instead of relying on inference.
3. **`/api/aggregate` running a live query every time.** Right now it does
   a `GROUP BY` over the whole `normalized_events` table on every request.
   That's fine at a few thousand rows, but at millions of rows with
   frequent polling, this would need pre-computed rollup tables instead
   of scanning everything fresh each time.
4. **Everything happening inside the request itself.** Ingestion and
   normalization both run synchronously as part of the HTTP request right
   now. Under heavy traffic this would be better split into a fast
   write-ahead log + a background worker doing the actual normalization,
   but I intentionally didn't build that out since the assignment said not
   to over-engineer the infrastructure.

## About the UI

- The preset buttons on the form let you quickly try: a clean payload, a
  messy one (comma in the amount, slashes in the date), a payload from an
  unfamiliar client with different field names, and a broken one missing
  `source` to see it get rejected.
- Checking "Simulate DB failure mid-write" and submitting shows the
  `failed_write` status appear in the table. Then hitting "Retry same
  event" shows it resolve to `processed` without creating a second row.
- "Processed / Failed Events" is the raw audit log of everything that hit
  the ingestion endpoint. "Aggregated Results" only reflects what actually
  made it into the canonical store.
