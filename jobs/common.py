"""Shared configuration and helpers for the Medallion pipeline (Bronze -> Silver -> Gold).

Used by the Airflow jobs (spark-submit / python), the manual prep scripts and the notebooks.
"""

import itertools
import os
import socket
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from botocore.client import Config

# ---------------------------------------------------------------- environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# ---------------------------------------------------------------- data source
SOURCE_BUCKET_PATH = "datasets-documentation/stackoverflow/parquet"
SOURCE_REGION = "eu-west-3"
SOURCE_BASE_URL = f"https://{SOURCE_BUCKET_PATH.split('/')[0]}.s3.{SOURCE_REGION}.amazonaws.com/stackoverflow/parquet"
USERS_SOURCE_URL = f"{SOURCE_BASE_URL}/users.parquet"

# Volume window: January 1st to June 30th (inclusive) of each year, for every table.
WINDOW_START = (1, 1)
WINDOW_END_EXCLUSIVE = (7, 1)

# ---------------------------------------------------------------- lake layout
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"
BUCKETS = (BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET)

BRONZE_POSTS = {
    2020: f"{BRONZE_BUCKET}/post/2020/post_2020.parquet",
    2021: f"{BRONZE_BUCKET}/post/2021/post_2021.parquet",
}
BRONZE_USERS = {
    2020: f"{BRONZE_BUCKET}/users/2020/users_2020.parquet",
    2021: f"{BRONZE_BUCKET}/users/2021/users_2021.parquet",
}

CATALOG = "nessie"
POST_HIST = f"{CATALOG}.silver.post_hist"
USERS_HIST = f"{CATALOG}.silver.users_hist"
GOLD_TABLE = f"{CATALOG}.gold.cant_post_x_user_hist"
POST_HIST_LOCATION = f"s3://{SILVER_BUCKET}/post_hist"
USERS_HIST_LOCATION = f"s3://{SILVER_BUCKET}/users_hist"
GOLD_LOCATION = f"s3://{GOLD_BUCKET}/cant_post_x_user_hist"


def window_bounds(year):
    """Return (start, end_exclusive) as tz-aware UTC datetimes for the year's window."""
    start = datetime(year, *WINDOW_START, tzinfo=timezone.utc)
    end = datetime(year, *WINDOW_END_EXCLUSIVE, tzinfo=timezone.utc)
    return start, end


# ---------------------------------------------------------------- S3 / MinIO
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_buckets(client=None):
    client = client or s3_client()
    existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
    for bucket in BUCKETS:
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)
            print(f"Bucket {bucket} created")


def minio_fs():
    """pyarrow filesystem pointing at MinIO (used to stream-write parquet)."""
    scheme, host = MINIO_ENDPOINT.split("://", 1)
    return pafs.S3FileSystem(
        access_key=AWS_KEY,
        secret_key=AWS_SECRET,
        endpoint_override=host,
        scheme=scheme,
        region=AWS_REGION,
    )


def list_keys(client, bucket, prefix):
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


# ---------------------------------------------------------------- source reading
def open_source_parquet(relative_path):
    """Open a public ClickHouse StackOverflow parquet file lazily (range requests, no full download)."""
    source_fs = pafs.S3FileSystem(anonymous=True, region=SOURCE_REGION)
    return pq.ParquetFile(f"{SOURCE_BUCKET_PATH}/{relative_path}", filesystem=source_fs)


def _filter_row_group(relative_path, row_group, year, column, batch_size):
    """Read one row group (own file handle: thread-safe) and return only the rows in the window."""
    start, end = window_bounds(year)
    start_s, end_s = pa.scalar(start, pa.timestamp("ms", "UTC")), pa.scalar(end, pa.timestamp("ms", "UTC"))
    parquet_file = open_source_parquet(relative_path)
    kept = []
    for batch in parquet_file.iter_batches(batch_size=batch_size, row_groups=[row_group]):
        table = pa.Table.from_batches([batch])
        mask = pc.and_(pc.greater_equal(table[column], start_s), pc.less(table[column], end_s))
        filtered = table.filter(mask)
        if filtered.num_rows:
            kept.append(filtered)
    return kept


def iter_window_batches(relative_path, year, column="CreationDate", batch_size=100_000, workers=4):
    """Yield pyarrow Tables with only the rows whose `column` falls inside the year's window.

    Row groups are fetched with a few parallel range requests (the source link is
    per-connection bound) and consumed through a sliding window, so memory stays bounded
    regardless of file size. Row groups whose min/max statistics miss the window are skipped.
    """
    start, end = window_bounds(year)
    meta = open_source_parquet(relative_path)
    col_idx = meta.schema_arrow.get_field_index(column)
    groups = []
    for rg in range(meta.metadata.num_row_groups):
        stats = meta.metadata.row_group(rg).column(col_idx).statistics
        if stats is not None and stats.has_min_max and (stats.max < start or stats.min >= end):
            continue
        groups.append(rg)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = deque()
        todo = iter(groups)
        for rg in itertools.islice(todo, workers):
            pending.append(pool.submit(_filter_row_group, relative_path, rg, year, column, batch_size))
        while pending:
            tables = pending.popleft().result()
            nxt = next(todo, None)
            if nxt is not None:
                pending.append(pool.submit(_filter_row_group, relative_path, nxt, year, column, batch_size))
            yield from tables


# ---------------------------------------------------------------- Spark
def get_spark(app_name):
    """SparkSession wired to Nessie (Iceberg catalog) and MinIO (S3).

    Master and driver host come from the environment so the same code runs from
    spark-submit (Airflow) and from Jupyter, always against the standalone cluster.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.uri", NESSIE_URI)
        .config(f"spark.sql.catalog.{CATALOG}.ref", "main")
        .config(f"spark.sql.catalog.{CATALOG}.authentication.type", "NONE")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{GOLD_BUCKET}/")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", AWS_KEY)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", AWS_SECRET)
        .config(f"spark.sql.catalog.{CATALOG}.client.region", AWS_REGION)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Iceberg vectorized (Arrow/netty) reads crash executors on JDK 17 without extra JVM flags
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    master = os.getenv("SPARK_MASTER")
    if master:
        builder = builder.master(master)
        builder = builder.config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", socket.gethostname()))
        builder = builder.config("spark.driver.bindAddress", "0.0.0.0")
        builder = builder.config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "3g"))
        builder = builder.config("spark.cores.max", os.getenv("SPARK_CORES_MAX", "3"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def expire_old_snapshots(spark, table):
    """Keep only the current snapshot: MERGE rewrites data files on every run, so without this
    the old versions would pile up in MinIO. Time travel is intentionally sacrificed."""
    # Nessie creates tables with gc.enabled=false; this project uses a single branch, so GC is safe.
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('gc.enabled' = 'true', "
        "'write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '2')"
    )
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{table.split('.', 1)[1]}', "
        f"older_than => TIMESTAMP '{now}', retain_last => 1)"
    ).show(truncate=False)


def require_tables(spark, *tables):
    missing = [t for t in tables if not spark.catalog.tableExists(t)]
    if missing:
        raise RuntimeError(
            f"Missing tables {missing}. Run the manual prep first "
            "(prepare_bronze_manual + silver_post_hist_manual), see README."
        )
