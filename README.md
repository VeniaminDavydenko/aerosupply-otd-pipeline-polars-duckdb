# Supplier OTD Pipeline

A small, scheduled Python pipeline that turns raw purchase-order lines into a
daily supplier on-time-delivery (OTD) report. One script, four stages:
**clean → metrics → validate → output.** Structured below around the six
[CRISP-DM](https://en.wikipedia.org/wiki/Cross-industry_standard_process_for_data_mining)
phases.

**Stack:** just two tools — **Polars** (clean + write) and **DuckDB** (SQL metrics).

```
python pipeline.py
```

Reads `../data/purchase_order_lines.csv` and `../data/suppliers.csv`; writes one
timestamped Excel file per metric plus a Parquet extract into `output/`, and a
line into `pipeline_log.txt`.

## 1 · Business Understanding

Buyers need a daily view of which suppliers are slipping, so they know who to
call before a late part becomes a stopped production line. The pipeline answers
three questions every morning: **who is late, is it getting worse, and what is
overdue right now.**

## 2 · Data Understanding

One row per purchase-order line in `purchase_order_lines.csv` (~1,480 lines):
`po_number`, `part_id`, `supplier_id`, `order_date`, `promised_date`,
`received_date`, `qty_ordered`, `qty_received`, `unit_price`. A line with no
`received_date` is still **open** (not yet delivered). Supplier names live in a
separate `suppliers.csv`, joined by `supplier_id`. The data is **synthetic** and
frozen in the past (receipts stop 2026-05-20), so overdue logic is pinned to an
`ASOF` date of 2026-06-01 for reproducible results — the same as-of date used by
the project's SQL analysis.

## 3 · Data Preparation (Stage 1 — Polars)

Loads the CSV **lazily** (`scan_csv`), parses the two date columns, and derives
`days_late` (receipt − promise) and an `on_time` flag (`days_late <= 0`). Open
lines keep null dates on purpose — they're needed for the overdue metric.
Cancelled lines are filtered *if* a status column exists (a documented no-op
here — this dataset has none). **Why Polars:** lazy evaluation plans the whole
parse/derive/filter as one pass and only touches the columns used — the habit
that scales to millions of rows.

## 4 · Modeling / Analysis (Stage 2 — DuckDB)

DuckDB reads the cleaned Polars frame *by variable name* (no copy, no database)
and runs three SQL queries, each returned as a Polars frame:

| Query | Question it answers |
|-------|---------------------|
| **Supplier OTD scorecard** | Who is late? Lines, on-time lines, OTD %, avg days late — ranked worst to best. |
| **Monthly OTD trend** | Is delivery improving or sliding? OTD % by month across all suppliers. |
| **Delinquency aging** | What's overdue now? Open lines past promise, bucketed 1–15 / 16–30 / 31–60 / 60+ days, with line counts and open-dollar value per supplier. |

`FILTER` clauses do the conditional counts (on-time lines, lateness on late
lines only) inline, which reads closer to the business question than nested
`CASE` sums.

## 5 · Evaluation (Stage 3 — Validate)

Before writing anything, two cheap checks: row count is above a sane floor
(500), and there are **zero nulls** in the columns everything keys on —
`supplier_id`, `promised_date`, `po_number`. **Fail fast:** if a check fails the
pipeline appends a `FAIL` line to the log, prints the reason, and exits
non-zero — so a scheduler flags the run instead of shipping a confident-looking
wrong report. A halved feed or dropped supplier IDs is caught here, not by a
buyer staring at a broken scorecard.

## 6 · Deployment (Windows Task Scheduler)

Run it every morning before buyers start:

1. **Task Scheduler → Create Basic Task** → name it "Supplier OTD Pipeline".
2. **Trigger:** Daily, e.g. 6:00 AM.
3. **Action:** *Start a program.*
   - Program/script: `python`
   - Arguments: `pipeline.py`
   - Start in: the full path to this `pipeline/` folder.
4. Finish. Right-click the task → **Run** once to confirm it works.

**What the log proves:** each run appends one line to `pipeline_log.txt` —
`PASS` with rows processed and overall OTD %, or `FAIL` with the reason and a
non-zero exit. It's the paper trail that the report is fresh and that a missing
morning report was a real failure, not a silently skipped run.
