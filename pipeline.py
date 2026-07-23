"""Daily supplier on-time-delivery report: clean, metrics, validate, output."""
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import duckdb

PIPE_DIR = Path(__file__).resolve().parent
ROOT = PIPE_DIR.parent
DATA = ROOT / "data"
OUT = PIPE_DIR / "output"
LOG = PIPE_DIR / "pipeline_log.txt"

ASOF = date(2026, 6, 1)     # dataset is frozen in the past, so pin "today"
MIN_ROWS = 500
RUN_DATE = date.today().isoformat()
STAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(line: str) -> None:
    with open(LOG, "a") as f:
        f.write(line + "\n")


# Clean
clean_df = (
    pl.scan_csv(DATA / "purchase_order_lines.csv")
    .with_columns(
        pl.col("promised_date").str.to_date(strict=False),
        pl.col("received_date").str.to_date(strict=False),
    )
    .with_columns(
        (pl.col("received_date") - pl.col("promised_date")).dt.total_days().alias("days_late"),
    )
    .with_columns(
        (pl.col("days_late") <= 0).alias("on_time"),
    )
    .collect()
)
suppliers_df = pl.read_csv(DATA / "suppliers.csv")


# Metrics
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


# Validate
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
    sys.exit(1)


# Output
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
