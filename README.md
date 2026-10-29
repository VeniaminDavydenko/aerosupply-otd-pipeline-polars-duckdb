# Supplier OTD Pipeline (moved)

This project now lives inside the AeroSupply analysis repo, next to the SQL analysis that uses the same purchase-order data:

**https://github.com/VeniaminDavydenko/aerosupply-supply-chain-analytics** (see the `pipeline/` folder)

The pipeline is a daily Polars and DuckDB report that rebuilds the supplier on-time-delivery scorecard, monthly trend and delinquency aging, and refuses to publish a report built on bad input. The code, its history and its tests moved there; this repository is archived and kept only so old links keep working.
