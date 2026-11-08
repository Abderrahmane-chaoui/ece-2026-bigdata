"""Lab: Govern Iceberg tables with Polaris (cells of the lab gathered in one script).

Usage (the principal is chosen like POLARIS_PRINCIPAL in the lab):
  STEP=dev       POLARIS_PRINCIPAL=dev     spark-submit ... polaris_lab/lab_polaris.py
  STEP=analyst   POLARIS_PRINCIPAL=analyst spark-submit ... polaris_lab/lab_polaris.py
  STEP=dev_after POLARIS_PRINCIPAL=dev     spark-submit ... polaris_lab/lab_polaris.py
  STEP=reader    POLARIS_PRINCIPAL=silver_analyst spark-submit ... polaris_lab/lab_polaris.py
  STEP=cleanup   POLARIS_PRINCIPAL=dev     spark-submit ... polaris_lab/lab_polaris.py
"""
import os

from pyspark.sql import SparkSession

STEP = os.environ["STEP"]
POLARIS_PRINCIPAL = os.environ.get("POLARIS_PRINCIPAL", "dev")  # "dev" or "analyst"
POLARIS_CREDENTIAL = os.environ[f"POLARIS_{POLARIS_PRINCIPAL.upper()}_CREDENTIAL"]

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-polaris")
    .master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    # S3 access, identical to the Spark lab
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Hadoop catalog of the Spark lab
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    # Polaris catalog, REST API, principal chosen by POLARIS_PRINCIPAL
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Polaris principal:", POLARIS_PRINCIPAL, "| step:", STEP)
print("fs.s3.impl:", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))


def title(text):
    print(f"\n===== {text} =====", flush=True)


def sql(query, show=True, truncate=False):
    print(">>>", " ".join(query.split()))
    df = spark.sql(query)
    if show:
        df.show(50, truncate=truncate)
    return df


def try_sql(query):
    """Run a statement that may be refused by Polaris and print the error."""
    try:
        sql(query)
        print("-> SUCCEEDED")
    except Exception as exc:  # noqa: BLE001
        lines = [l.strip() for l in str(exc).splitlines()]
        causes = [l for l in lines
                  if ("Exception:" in l or "Error:" in l) and "Py4J" not in l]
        print("-> FAILED:", type(exc).__name__)
        for line in (causes or lines)[:3]:
            print("   ", line[:300])


if STEP == "dev":
    title("Both catalogs are reachable")
    sql("SHOW NAMESPACES IN lab")
    sql("SHOW NAMESPACES IN polaris")

    title("Register the silver table in Polaris")
    version = os.environ["METADATA_VERSION"]
    sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    already = spark.sql("SHOW TABLES IN polaris.silver").filter("tableName = 'orders'").count() > 0
    if not already:
      sql(f"""
        CALL polaris.system.register_table(
          table => 'silver.orders',
          metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{version}.metadata.json'
        )
      """)
    else:
        print("silver.orders is already registered (previous run)")
    sql("SELECT count(*) AS orders FROM polaris.silver.orders")

    title("Create tables as dev")
    sql("SHOW TABLES IN polaris.silver")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")
    stg_users = spark.table("lab.silver.users")
    if spark.sql("SHOW TABLES IN polaris.silver").filter("tableName = 'users'").count() == 0:
        stg_users.writeTo("polaris.silver.users").create()
    sql("SELECT count(*) AS users FROM polaris.silver.users")
    sql("SELECT file_path FROM polaris.silver.users.files")

    title("Schema change as dev")
    if "country" not in spark.table("polaris.silver.users").columns:
        sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    sql("DESCRIBE polaris.silver.users")
    sql("SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries")

elif STEP == "analyst":
    title("Read as the analyst")
    sql("SHOW TABLES IN polaris.silver")
    sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product")
    title("Write as the analyst (must be refused)")
    try_sql("INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    try_sql("ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

elif STEP == "dev_after":
    title("Snapshots of silver.orders after the failed INSERT of the analyst")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")
    sql("SELECT count(*) AS orders FROM polaris.silver.orders")

    title("Exercise 1: remove the country column")
    sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots")
    sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
    sql("DESCRIBE polaris.silver.users")
    sql("SELECT snapshot_id, operation FROM polaris.silver.users.snapshots")
    sql("SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries")

    title("Exercise 2: gold namespace created by dev")
    sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    sql("""CREATE TABLE IF NOT EXISTS polaris.gold.orders_per_product AS
           SELECT product, count(*) AS orders, sum(quantity) AS quantity
           FROM polaris.silver.orders GROUP BY product""")
    sql("SELECT * FROM polaris.gold.orders_per_product ORDER BY orders DESC")

elif STEP == "reader":
    title("Exercise 2: principal with silver_reader only")
    try_sql("SELECT count(*) AS orders FROM polaris.silver.orders")
    try_sql("SELECT count(*) AS users FROM polaris.silver.users")
    try_sql("SELECT * FROM polaris.gold.orders_per_product")
    try_sql("INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")

elif STEP == "cleanup":
    title("Cleanup")
    sql("DROP TABLE IF EXISTS polaris.silver.users")
    sql("DROP TABLE IF EXISTS polaris.gold.orders_per_product")
    sql("DROP NAMESPACE IF EXISTS polaris.gold")
    sql("SHOW TABLES IN polaris.silver")

spark.stop()
print("\nDONE")
