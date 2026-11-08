# Lab dbt: Bronze to Silver with dbt and DuckDB

Author: CHAOUI Abderrahmane, ING4 Group 3

Results of my run: 50 users, 2,829 orders (8,615 items) in `bronze/`. `dbt build` gives
PASS=29, WARN=1, ERROR=0 after the exercises.

## Bronze layer

**The bronze files are left untouched. What is the benefit when a bug is found six months later?**
The raw data is still there exactly as it was received. I fix the SQL of the silver model and run
`dbt build` again: the whole silver layer is rebuilt from the original files, with no need to ask the
producer to send the data again. If bronze had been modified in place, the bug would be in the data
forever.

**The source reads the whole file on every query. Which format and layout for hourly orders over years?**
Parquet (columnar, compressed, typed, with statistics) instead of CSV, partitioned by date, for example
`bronze/orders/date=2026-11-08/hour=17/part-0.parquet`, or an Iceberg table. A query on one day then
reads only the matching partitions and only the needed columns, and the model can be `incremental`
to process only the new partitions.

## Silver layer

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**
A view stores only the SQL. Each query runs the transformation again, here it re-reads the CSV files
on S3. A view is preferable when the data is small, changes often, or is rarely queried: it costs no
storage and is always up to date. A table is better when the model is queried often or is expensive.

**UUID values use 16 bytes. How many bytes for the VARCHAR representation?**
The text form `bdd640fb-0667-4ad1-9c80-317fa3b1799d` has 36 characters, so 36 bytes (plus the string
overhead), more than twice the 16 bytes of the `UUID` type.

## Data tests

**Failing test investigation.** `assert_orders_have_users` passes on my data (0 orphan orders).
`assert_orders_after_user_birth` failed with 288 rows: the orders are dated 2020 while Faker generates
birthdates relative to today, so some users are "born" after their first order. I first set the test
to `severity='warn'`, then fixed it in exercise 3.

**Which strategy for a dashboard of customer ages? Of sales per region?**
For the ages, the invalid birthdates must not be used: I replace them with `NULL` (and flag them), so
the age is unknown instead of wrong or negative. For the sales per region, the birthdate is not used at
all, so the orders must be kept: rejecting the rows would remove real sales. A warning test is enough.

**The addresses associate states with random zip codes. Which test, and which reference data?**
A test that checks that the zip code belongs to the ZIP ranges of its state (exercise 4). It needs a
reference table of the valid ZIP ranges (or prefixes) per state, coming from USPS.

## Parquet export

`users.parquet` (7 KB) and `orders.parquet` (72 KB) were written in `s3://<bucket>/silver/dbt/`.
For `stg_orders`, the Parquet file and the view give the same result: 2,829 rows, 8,615 items,
2,829 distinct `order_id`, and `view EXCEPT parquet` returns 0 rows.
The view is a logical materialization (the query runs at each read), the Parquet file a physical one
(the result is computed once at export, then only read).

## Exercises

### 1. `username_normalized`
`lower(trim(username))` in `stg_users`, documented in `silver.yml`, with `unique` and `not_null` tests
(two users must not have the same normalized username).

### 2. Order validation
- `quantity` positive: singular test `tests/assert_orders_quantity_positive.sql` (no generic test for
  "greater than 0" without a package such as dbt_utils), plus `not_null` and `accepted_values` 1 to 5.
- `product` in the expected list: generic `accepted_values` + `not_null`.
- every `user_id` exists in `stg_users`: generic `relationships` test (and the singular
  `assert_orders_have_users`).

### 3. Invalid birthdates
`stg_users` computes the first order of each user from `stg_orders`, and
`birthdate_is_valid = birthdate <= first order` (true when the user has no order). An invalid birthdate
is replaced by `NULL`. On my data: 45 valid, 5 invalid. `assert_orders_after_user_birth` now passes, so
I removed `severity='warn'`: it is a real error again if the rule breaks. A new test
`assert_birthdate_is_valid_consistent` checks that an invalid birthdate is always `NULL`.

### 4. Invalid ZIP codes
Reference data `seeds/state_zip_ranges.csv` (state, zip_min, zip_max, from the USPS ZIP prefixes,
including territories and military codes AA/AE/AP) and the singular test
`tests/assert_zip_code_matches_state.sql` (severity warn). Result: **49 of the 50 users** have a ZIP
code outside the ranges of their state, which confirms that Faker generates them randomly.
- Where should the data come from? From an official source (USPS / Census ZCTA files), not written
  by hand.
- As a dbt seed? Yes for a small, rarely changing table like this one (versioned with the code and
  reviewed in Git). A large or frequently updated table would rather be a source loaded by ingestion.
- Validation in silver? Yes, silver is where data quality is checked. I keep it as a warning and do
  not drop the rows, since the orders of these users are still real.
- When the reference changes, the seed must be updated (`dbt seed`) and the tests run again; old
  results can change, so the version of the reference should be traced (Git history of the seed).

### 5. Materialization experiment
With `+materialized: table`, `dbt build --select silver` created `main_silver.stg_users` and
`stg_orders` as `BASE TABLE` (instead of `VIEW`) and the DuckDB file grew from 799 KB to 1.32 MB.
- Data storage: in the `lab_medallion.duckdb` file for a table; only the SQL is stored for a view.
- Transformation time: at `dbt build` for a table; at each query for a view.
- CSV changes: a view sees them immediately; a table stays stale until the next `dbt build`.
- More storage: the table.
- Avoids re-reading the source files at every query: the table.
I restored `+materialized: view` afterwards.
