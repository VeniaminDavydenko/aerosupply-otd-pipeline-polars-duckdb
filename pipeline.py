"""
Supplier OTD pipeline — daily on-time-delivery report for buyers.

Four stages: clean (Polars) -> metrics (DuckDB) -> validate -> output.
Reads the raw PO lines + supplier lookup, writes timestamped Excel files and a
Parquet extract into output/, and logs one line to pipeline_log.txt.

Built on two tools only: Polars (clean + write) and DuckDB (SQL metrics).

Run:  python pipeline.py

Overdue logic is pinned to ASOF because the dataset is synthetic and frozen in
the past; filenames and the log use the real run date. See STUDY_GUIDE.docx for
the full walkthrough of every stage and why it's built this way.
"""
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import duckdb

# Paths resolve off this file so the script runs from any directory.
PIPE_DIR = Path(__file__).resolve().parent
ROOT = PIPE_DIR.parent                          # repo root; data/ lives here
DATA = ROOT / "data"
OUT = PIPE_DIR / "output"
LOG = PIPE_DIR / "pipeline_log.txt"

ASOF = date(2026, 6, 1)     # pinned "today" for overdue logic
MIN_ROWS = 500              # validation floor (real file is ~1,480 lines)
RUN_DATE = date.today().isoformat()
STAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(line: str) -> None:
    with open(LOG, "a") as f:
        f.write(line + "\n")


# ---- Stage 1 · Clean (Polars, lazy) ----------------------------------------
lazy = pl.scan_csv(DATA / "purchase_order_lines.csv").with_columns(
    pl.col("promised_date").str.to_date(strict=False),
    pl.col("received_date").str.to_date(strict=False),   # empty -> null (open lines)
).with_columns(
    (pl.col("received_date") - pl.col("promised_date")).dt.total_days().alias("days_late"),
).with_columns(
    (pl.col("days_late") <= 0).alias("on_time"),
)

# Drop cancelled lines if the feed ever grows a status column (no-op today).
if "status" in lazy.collect_schema().names():
    lazy = lazy.filter(pl.col("status") != "Cancelled")

clean_df = lazy.collect()
suppliers_df = pl.read_csv(DATA / "suppliers.csv")       # small name lookup


# ---- Stage 2 · Metrics (DuckDB reads the Polars frames by name) ------------
# Received lines only; FILTER does the conditional count/avg inline.
scorecard_df = duckdb.sql("""
    SELECT
        s.supplier_name,
        COUNT(*)                                              AS total_lines,
        COUNT(*) FILTER (WHERE c.on_time)                     AS on_time_lines,
        ROUND(100.0 * COUNT(*) FILTER (WHERE c.on_time) / COUNT(*), 1) AS otd_pct,
        ROUND(AVG(c.days_late) FILTER (WHERE NOT c.on_time), 1)        AS avg_days_late
    FROM clean_df c
    JOIN suppliers_df s USING (supplier_id)
    WHERE c.received_date IS NOT NULL
    GROUP BY s.supplier_name
    ORDER BY otd_pct ASC
""").pl()

monthly_df = duckdb.sql("""
    SELECT
        date_trunc('month', received_date)::date              AS month,
        COUNT(*)                                              AS lines_received,
        ROUND(100.0 * COUNT(*) FILTER (WHERE on_time) / COUNT(*), 1) AS otd_pct
    FROM clean_df
    WHERE received_date IS NOT NULL
    GROUP BY 1
    ORDER BY 1
""").pl()

# Open lines already past promise as of ASOF, bucketed by days overdue.
aging_df = duckdb.sql(f"""
    SELECT
        s.supplier_name,
        CASE
            WHEN DATE '{ASOF}' - c.promised_date BETWEEN 1 AND 15  THEN '1-15'
            WHEN DATE '{ASOF}' - c.promised_date BETWEEN 16 AND 30 THEN '16-30'
            WHEN DATE '{ASOF}' - c.promised_date BETWEEN 31 AND 60 THEN '31-60'
            ELSE '60+'
        END                                                   AS overdue_bucket,
        COUNT(*)                                              AS open_lines,
        ROUND(SUM(c.qty_ordered * c.unit_price), 0)           AS open_value_usd
    FROM clean_df c
    JOIN suppliers_df s USING (supplier_id)
    WHERE c.received_date IS NULL
      AND c.promised_date < DATE '{ASOF}'
    GROUP BY s.supplier_name, overdue_bucket
    ORDER BY s.supplier_name, open_value_usd DESC
""").pl()


# ---- Stage 3 · Validate (fail fast, before writing anything) ---------------
reason = None
if clean_df.height < MIN_ROWS:
    reason = f"row count {clean_df.height} below floor {MIN_ROWS}"
else:
    nulls = clean_df.select(
        pl.col("supplier_id").is_null().sum().alias("supplier_id"),
        pl.col("promised_date").is_null().sum().alias("promised_date"),
        pl.col("po_number").is_null().sum().alias("po_number"),
    ).row(0, named=True)
    bad = {col: n for col, n in nulls.items() if n > 0}
    if bad:
        reason = f"null values in required columns: {bad}"

if reason:
    log(f"{STAMP} | FAIL | {reason}")
    print(f"VALIDATION FAILED: {reason}")
    sys.exit(1)     # non-zero so the scheduler flags the run


# ---- Stage 4 · Output + log ------------------------------------------------
# Each metric is written to its own Excel file with a plain one-line Polars call,
# plus the scorecard as Parquet for downstream tools.
OUT.mkdir(exist_ok=True)
scorecard_df.write_excel(OUT / f"otd_scorecard_{RUN_DATE}.xlsx")
monthly_df.write_excel(OUT / f"monthly_otd_trend_{RUN_DATE}.xlsx")
aging_df.write_excel(OUT / f"delinquency_aging_{RUN_DATE}.xlsx")
scorecard_df.write_parquet(OUT / f"supplier_scorecard_{RUN_DATE}.parquet")

received = clean_df.filter(pl.col("received_date").is_not_null())
overall_otd = round(100.0 * received["on_time"].sum() / received.height, 1)

log(f"{STAMP} | PASS | rows_processed={clean_df.height} | overall_otd={overall_otd}%")

print(f"OK — {clean_df.height} rows, overall OTD {overall_otd}%")
print(f"  Wrote 3 Excel files + 1 Parquet to {OUT}")
