# Polaris labs: deployment and governance of Iceberg tables

Author: Abderrahmane CHAOUI (namespace `user-a-chaoui-ece`)

Files in this folder:

| File | Content |
| --- | --- |
| `lab_polaris.py` | Lab cells in one script, run step by step with `STEP` and `POLARIS_PRINCIPAL` |
| `run.sh` | Exports S3 and Polaris credentials, then runs `spark-submit` for one step |
| `exercise2_rbac.sh` | Exercise 2: creates `silver_reader`, `silver_reader_role` and `silver_analyst` through the management API |
| `output_*.txt` | Output of each step (`dev`, `analyst`, `dev_after`, `exercise2_rbac`, `reader`, `cleanup`) |

The Helm chart of the deployment lab is in `polaris/deployment/`.

## Lab 1: deployment

- **What runs:** `helm install polaris polaris/deployment` starts PostgreSQL (Polaris persistence), the Polaris server, and a bootstrap job.
  - Catalog API: `http://polaris:8181`
  - Management API: `http://polaris-mgmt:8182`
- **What the bootstrap job does,** with the `root` token (realm `POLARIS`):
  1. Creates the catalog `user-a-chaoui-ece`. Its base location is `s3://user-a-chaoui-ece/warehouse/`.
  2. Creates the principals `dev` and `analyst`.
  3. Creates the principal roles `data_engineer` and `data_analyst`.
  4. Creates the catalog roles `catalog_contributor` and `catalog_reader`.
  5. Grants privileges and wires the roles together:
     - `dev -> data_engineer -> catalog_contributor`, with `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA` and `CATALOG_MANAGE_CONTENT`.
     - `analyst -> data_analyst -> catalog_reader`, with `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`.
- **Credentials:** the client id and secret of `dev` and `analyst` were extracted to `~/.polaris-credentials.env` and are not committed. They are read by `run.sh`.
- **Why a catalog server:** with the Hadoop catalog of the Spark lab, the "catalog" is just the `version-hint.text` file on S3.
  - Anyone with S3 access can read or write every table.
  - There is no locking across engines.
  - With Polaris, every engine (Spark, DuckDB, Trino, …) goes through one REST API. The API checks the principal's privileges and commits metadata atomically.

## Lab 2: govern Iceberg tables

### Registering the existing table

`CALL polaris.system.register_table('silver.orders', '.../orders/metadata/v3.metadata.json')` registers the table written by the Spark lab without copying any data. Polaris only stores a pointer to the metadata file.

- The full history of the Spark lab is kept: 3 snapshots, `overwrite`, `overwrite`, `append`.
- `count(*)` returns 2830 rows (2829 orders plus the row appended in Spark exercise 4).
- After registration, writes should only go through Polaris. The Hadoop catalog `lab` and Polaris would otherwise each move their own pointer, and the two would diverge.

### Creating a table as `dev`

`stg_users.writeTo("polaris.silver.users").create()` creates a managed table: 50 rows.

- Polaris chose the location from the catalog's base location: `s3://user-a-chaoui-ece/warehouse/silver/users/`.
- That is the same folder as the Hadoop table `lab.silver.users`. Both tables coexist, because Polaris names its metadata files `00000-<uuid>.metadata.json` and the Hadoop catalog uses `vN.metadata.json` plus `version-hint.text`.

### Schema change as `dev`

`ALTER TABLE polaris.silver.users ADD COLUMN country STRING` succeeds.

- It only writes a new metadata file. No new snapshot is created and no data file is rewritten.
- `metadata_log_entries` shows the chain of metadata files (00000, 00001, …).

### Reading and writing as `analyst`

- **Reads work:**
  - `SHOW TABLES IN polaris.silver` lists `users` and `orders`.
  - The `GROUP BY product` on orders returns croissant 492, drink 484, brioche 468, cookie 466, donut 462, bread 458.
- **Writes are refused by Polaris:**

```
INSERT INTO polaris.silver.orders ...
-> org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Principal 'analyst' with activated
   PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not
   authorized for op ADD_TABLE_SNAPSHOT
ALTER TABLE polaris.silver.users ADD COLUMN phone STRING
-> ForbiddenException: Forbidden: Principal 'analyst' ... is not authorized for op ADD_TABLE_SCHEMA
```

- **Why the INSERT fails at commit:**
  - Spark first loads the table, which is allowed with `TABLE_READ_DATA`.
  - It then writes Parquet files. With `HadoopFileIO`, Spark uses our own S3 credentials, not credentials vended by Polaris.
  - It finally asks Polaris to commit the new snapshot. That commit is refused, so Spark aborts the write ("IcebergBatchWrite … is aborting").
  - Any orphan data files are never referenced by a snapshot, so readers never see them.
- **Check as `dev` afterwards** (`output_dev_after.txt`): `silver.orders` still has its 3 snapshots and 2830 rows. The failed INSERT left no trace in the table.

### Exercise 1: remove the `country` column

`ALTER TABLE polaris.silver.users DROP COLUMN country`, run as `dev`:

- `DESCRIBE` no longer shows `country`.
- `snapshots` is unchanged (one `overwrite` snapshot, from the create). Dropping a column is a metadata-only change, like adding one.
- `metadata_log_entries` gains one entry, giving 3 metadata files in total (00000 create, 00001 add column, 00002 drop column).
- Iceberg identifies columns by id, not by name. Old data files still physically contain the field, but it is no longer projected. If a column with the same name is added later, it gets a new id and does not resurrect the old values.

### Exercise 2: a reader limited to the `silver` namespace

`exercise2_rbac.sh` uses the management API with the `root` token. Every call returned 201.

1. It creates the catalog role `silver_reader`, with grants at namespace level on `["silver"]`, not at catalog level:
   - `NAMESPACE_READ_PROPERTIES`
   - `TABLE_LIST`
   - `TABLE_READ_PROPERTIES`
   - `TABLE_READ_DATA`
2. It creates the principal role `silver_reader_role` and attaches `silver_reader` to it for this catalog.
3. It creates the principal `silver_analyst`, assigns it `silver_reader_role`, and stores its credentials in `~/.polaris-credentials.env`.

To test it, `dev` created a `gold` namespace and a table `gold.orders_per_product`, then the `reader` step ran as `silver_analyst` (`output_reader.txt`):

| Statement as `silver_analyst` | Result |
| --- | --- |
| `SELECT count(*) FROM polaris.silver.orders` | SUCCEEDED |
| `SELECT count(*) FROM polaris.silver.users` | SUCCEEDED |
| `SELECT * FROM polaris.gold.orders_per_product` | Forbidden: Principal 'silver_analyst' with activated PrincipalRoles '[silver_reader_role]' … is not authorized for op LOAD_TABLE |
| `INSERT INTO polaris.silver.orders …` | Forbidden … is not authorized for op ADD_TABLE_SNAPSHOT (commit refused) |

Privileges granted on a namespace apply to that namespace and everything below it. They do not reach a sibling namespace like `gold`. Because no write privilege was granted, writes stay refused even inside `silver`. By contrast, `catalog_reader` (the role of `analyst`) is granted at catalog level, so it covers every namespace, including `gold`.

### Cleanup

The `cleanup` step, run as `dev`:

- dropped `silver.users`, `gold.orders_per_product` and the `gold` namespace;
- kept `silver.orders` registered (`SHOW TABLES IN polaris.silver` now shows only `orders`).

The Iceberg metadata of `orders` and `users` is copied into `warehouse/silver/*/metadata` and committed with this lab.
