"""Lab: Bronze to silver with Spark DataFrames, exercises only.

The silver layer is read back from the Parquet and Iceberg outputs of lab_pyspark_silver.py,
so this script fits in the 2000Mi of the service (the full script was OOM-killed during the exercises).
Run with:
  spark-submit --driver-memory 512m --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
      spark_lab/lab_pyspark_exercises.py
"""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def title(text):
    print(f"\n===== {text} =====", flush=True)


os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Spark 4.1.1 / Scala 2.13 in the vscode-pyspark image
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[1]")
    # The service is limited to 2000Mi: keep the driver small
    .config("spark.driver.memory", "512m")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(spark, "Spark", spark.version)

users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
stg_users = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
stg_users.createOrReplaceTempView("stg_users")
print("silver:", stg_users.count(), "users,", stg_orders.count(), "orders")

# ================================================================= Exercises
if os.environ.get("SKIP_EXERCISES") != "1":
    # ------------------------------------------------------------ Exercise 1
    title("Exercise 1: birthdate_is_valid (DataFrame API and Spark SQL)")
    first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))
    users_valid_df = (
        stg_users.join(first_orders, "user_id", "left")
        .withColumn(
            "birthdate_is_valid",
            F.coalesce(F.col("birthdate").cast("timestamp") <= F.col("first_ordered_at"), F.lit(True)),
        )
        .drop("first_ordered_at")
    )
    stg_orders.createOrReplaceTempView("stg_orders")
    users_valid_sql = spark.sql("""
        SELECT u.*,
               coalesce(CAST(u.birthdate AS TIMESTAMP) <= f.first_ordered_at, true) AS birthdate_is_valid
        FROM stg_users u
        LEFT JOIN (
            SELECT user_id, min(ordered_at) AS first_ordered_at FROM stg_orders GROUP BY user_id
        ) f ON u.user_id = f.user_id
    """)
    users_valid_df.groupBy("birthdate_is_valid").count().show()
    diff = users_valid_df.select("user_id", "birthdate_is_valid").exceptAll(
        users_valid_sql.select("user_id", "birthdate_is_valid"))
    print("Rows that differ between DataFrame and SQL:", diff.count())

    # ------------------------------------------------------------ Exercise 2
    title("Exercise 2: zip_code with regexp_extract, Python UDF and pandas_udf")
    import re
    import pandas as pd
    from pyspark.sql.functions import udf, pandas_udf

    ZIP_RE = re.compile(r"([A-Z]{2}) ([0-9]{5})$")

    @udf("string")
    def zip_py(address):
        if address is None:
            return ""
        m = ZIP_RE.search(address.split("\n")[-1])
        return m.group(2) if m else ""

    @pandas_udf("string")
    def zip_pd(address: pd.Series) -> pd.Series:
        return address.fillna("").str.split("\n").str[-1].str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")

    def timeit(label, col, runs=5):
        df = users_bronze.select(col.alias("zip_code"))
        df.count()  # warm-up
        t0 = time.perf_counter()
        for _ in range(runs):
            df.agg(F.count("zip_code")).collect()
        print(f"{label:<16} {1000 * (time.perf_counter() - t0) / runs:8.1f} ms per run")
        return df

    try:
        a = timeit("regexp_extract", F.regexp_extract("address", "([A-Z]{2}) ([0-9]{5})$", 2))
        b = timeit("python udf", zip_py("address"))
        c = timeit("pandas_udf", zip_pd("address"))
        print("Same results:", a.exceptAll(b).count() == 0 and a.exceptAll(c).count() == 0)
    except Exception as exc:  # pandas_udf needs pandas and pyarrow
        print("Exercise 2 failed:", type(exc).__name__, str(exc)[:300])

    # ------------------------------------------------------------ Exercise 3
    title("Exercise 3: cache the bronze DataFrames")
    for label, cached in (("without cache", False), ("with cache", True)):
        if cached:
            users_bronze.cache().count()
            orders_bronze.cache().count()
        t0 = time.perf_counter()
        for _ in range(3):
            users_bronze.count()
            orders_bronze.groupBy("product").count().collect()
        print(f"{label:<14} {1000 * (time.perf_counter() - t0) / 3:8.1f} ms per run")
    users_bronze.unpersist()
    orders_bronze.unpersist()

    # ------------------------------------------------------------ Exercise 4
    title("Exercise 4: append to Iceberg and time travel")
    first_snapshot = spark.sql(
        "SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at DESC").first()[0]
    new_order = spark.sql("""
        SELECT uuid() AS order_id, user_id, current_timestamp() AS ordered_at,
               1 AS quantity, 'croissant' AS product
        FROM lab.silver.orders LIMIT 1
    """)
    new_order.writeTo("lab.silver.orders").append()
    spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
    print("Current orders:", spark.table("lab.silver.orders").count())
    spark.sql(f"SELECT count(*) AS orders_before_append FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()
    # Parquet directory: an append only adds a file, there is no history to go back to
    new_order.write.mode("append").parquet(f"s3a://{BUCKET}/silver/spark/orders_append_demo.parquet")
    print("A Parquet directory keeps no snapshot: the previous state cannot be read back.")

spark.stop()
print("\nDONE")
