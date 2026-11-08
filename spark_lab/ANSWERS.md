# Lab: Bronze to silver with Spark DataFrames

Author: CHAOUI Abderrahmane, ING4 Group 3

Files:
- `lab_pyspark_silver.py`: the cells of the lab (session, bronze, silver with DataFrame API and Spark SQL,
  Parquet, Iceberg). Output in `output.txt`.
- `lab_pyspark_exercises.py`: the 4 exercises. Output in `output_exercises.txt`.

Environment: Spark 4.1.1, Scala 2.13, `iceberg-spark-runtime-4.1_2.13:1.11.0`, `hadoop-aws-3.4.2` in
`$SPARK_HOME/jars`. The scripts are run with `spark-submit` instead of the interactive `pyspark` shell.

Note on memory: my service is limited to 2 CPU / 2000Mi (the lab asks for 4000Mi). The first run of the
complete script was OOM-killed (the container restarts with `reason: OOMKilled`). I reduced the driver
to 1g (512m for the exercises), `local[2]` and 4 shuffle partitions, and ran the exercises in a second
script that reads back the silver Parquet files.

## Results

| | dbt (DuckDB) | Spark |
|---|---|---|
| users | 50 | 50 |
| orders | 2,829 | 2,829 |
| sum(quantity) | 8,615 | 8,615 |
| `order_id`, `user_id` | `UUID` | `VARCHAR` |
| `ordered_at` | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` |
| output | one object per model | one directory of part-files |

Deduplication: 2,829 orders, 2,832 with the 3 duplicated rows, 2,829 after `row_number()`. The DataFrame
and Spark SQL versions give the same count and columns.

## SparkSession

**The credentials are temporary. What happens to a running job if the service is restarted?**
The session keys are copied into the Hadoop configuration when the session starts. If the service restarts,
the driver JVM is killed with the container, so the job dies and must be started again. If the token
expires while the job runs, S3A gets `403 / ExpiredToken` errors, because Spark does not refresh keys
given as static configuration. This happened to me: the container was restarted (OOM) and the job stopped.

**Why does `SimpleAWSCredentialsProvider` fail here?**
It only sends an access key and a secret key. Onyxia gives STS credentials, which are only valid with their
session token. `TemporaryAWSCredentialsProvider` adds `fs.s3a.session.token` to the requests;
without it S3 refuses the signature.

## Silver layer

**Explain the output of the state query.**
It counts the users per state and the users without a city. The users with `missing_city > 0` are in
`AA`, `AE` or `AP`: these military addresses (`DPO AP 09617`) have no city, so `city` is `NULL` after
replacing the empty string returned by `regexp_extract()`.

**DataFrame or SQL?**
For a one-off exploration, SQL is faster to write and to read. For a model reused by colleagues, the
DataFrame API is easier to test, to factorize in functions and to check (errors appear in the IDE, columns
are Python objects). Both compile to the same Catalyst plan, so performance is the same.

**Where else can `regexp_extract()` returning `""` produce a wrong result?**
For `state` and `zip_code`: when the address does not match, they are `""` and not `NULL`, so a `not null`
check passes, a `groupBy("state")` creates an empty group, and a join with a reference table silently
fails. In the zip UDF exercise I return `""` too to compare the 3 versions, but in the silver layer they
should be converted to `NULL` like `city`.

## Parquet

**How many part-files under `silver/spark/orders.parquet/`, and why?**
One `part-00000-...snappy.parquet` (139 KB) plus `_SUCCESS`. Spark writes one file per partition of the
DataFrame; after the shuffle of the window, adaptive query execution coalesced the small partitions into one.
On a bigger dataset there would be several files.

**What do you lose with `coalesce(1)` on a larger dataset?**
The parallelism of the write: one task writes everything, it is slow, can run out of memory, and readers
cannot read the file in parallel as well as several files.

## Iceberg

**Which files hold the data, which ones describe the table?**
In `silver/spark/orders.parquet/` there are only Parquet data files. In `warehouse/silver/orders/`, `data/`
holds the Parquet files and `metadata/` describes the table: `vN.metadata.json` (schema, partitioning,
list of snapshots), the manifest lists `snap-*.avro`, the manifests `*-m0.avro` (list of data files with
statistics) and `version-hint.text` (current version).

**A job overwrites the Parquet directory while a query reads it. What changes with Iceberg?**
With plain Parquet the reader can see a mix of old and new files, missing files or an error. With Iceberg
the writer creates new files then commits a new metadata version atomically: the reader keeps reading the
snapshot it started with and never sees a half-written table.

**With the Hadoop catalog, what can go wrong with two concurrent commits?**
The pointer is a file in the bucket. S3 has no atomic rename, so two jobs can both write `v3.metadata.json`
or update `version-hint.text`, and one commit is lost or the table is corrupted. A catalog service
(REST catalog like Polaris, Nessie, Glue) does the commit with an atomic compare-and-swap, and adds
access control and a single place to find the tables.

## Compare with the dbt lab

**Schemas.** dbt/DuckDB keeps `UUID` and `TIMESTAMP WITH TIME ZONE`; Spark has no UUID type, so I cast the
ids to `string`, and `ordered_at` is a `TIMESTAMP` (session time zone). Consumers lose the type check of the
UUID (any string is accepted) and the storage is bigger (36 bytes instead of 16); joining both outputs needs
a cast.

**Layout.** One object per model (dbt) is easier for a consumer: one path, one file. A directory of
part-files scales, because each task writes its own file in parallel; readers use a glob or a table format.

**Tests.** This lab only used `assert` on counts and columns. To get the same guarantees as dbt I would need
to write checks for unique / not null keys, accepted values and foreign keys (for example with a small
function, or a library like Great Expectations / Deequ / Soda).

**What plays the role of `ref()` and `source()`?** Nothing explicit: the order of the cells and the Python
variables (`users_bronze` → `users_typed` → `stg_users`). There is no dependency graph or lineage
documentation; the Iceberg catalog only gives the names of the tables.

**Build times.** `dbt build` took a few seconds; the Spark script takes about a minute, mostly the JVM
start, the download of the Iceberg jar and the S3 reads. They are not comparable: different engines,
different steps (dbt creates views and runs tests; Spark writes Parquet and Iceberg files), and fixed costs
dominate on such a small dataset.

**dbt + DuckDB or Spark?**
- daily batch of 10 GB: dbt + DuckDB, one machine is enough and it is simpler;
- daily batch of 10 TB: Spark (distributed processing over many executors);
- team of analysts who only know SQL: dbt (SQL models, tests and documentation), possibly with
  `dbt-spark` if the volume needs Spark.

## Exercises

1. **`birthdate_is_valid`**: first order per user (`groupBy` + `min`), left join, then
   `coalesce(birthdate <= first_ordered_at, true)`. Same result with the DataFrame API and Spark SQL
   (0 different rows): 45 valid, 5 invalid, exactly like the dbt lab. In dbt I also replaced the invalid
   birthdate by `NULL` and added tests.
2. **zip_code**: `regexp_extract` 392 ms, Python UDF 883 ms, `pandas_udf` 554 ms per run, same results.
   The built-in function stays in the JVM; the Python UDF sends each row to a Python worker; the pandas UDF
   sends Arrow batches, which is faster than row by row but still slower than a native function.
3. **cache**: same queries on the bronze DataFrames, 666 ms without cache and 415 ms with cache: the CSV are
   no longer re-read from S3 (in the Spark UI the stages read `InMemoryTableScan` instead of the CSV scan).
   The UI was disabled in my second script to save memory.
4. **append + time travel**: after `writeTo("lab.silver.orders").append()`, the table has 3 snapshots
   (`overwrite`, `overwrite`, `append`) and 2,830 orders; `VERSION AS OF` the previous snapshot returns 2,829.
   A Parquet directory only gets one more file: nothing records the previous state, so it cannot be read back.
