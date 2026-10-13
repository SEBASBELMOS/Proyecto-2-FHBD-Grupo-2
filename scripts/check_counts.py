"""Print the Nessie table list, row counts and last fecha_cargue (idempotency check) and MinIO bucket contents.

Usage: docker exec g2_jupyter python /opt/airflow/scripts/check_counts.py
"""

import sys

sys.path.insert(0, "/opt/airflow/jobs")

from pyspark.sql import functions as F

from common import BUCKETS, GOLD_TABLE, POST_HIST, USERS_HIST, get_spark, list_keys, s3_client

spark = get_spark("check_counts")
for schema in ("silver", "gold"):
    print(f"Tables in nessie.{schema}:", [r.tableName for r in spark.sql(f"SHOW TABLES IN nessie.{schema}").collect()])
for table in (POST_HIST, USERS_HIST, GOLD_TABLE):
    row = spark.table(table).agg(F.count("*").alias("n"), F.max("fecha_cargue").alias("loaded")).first()
    print(f"{table}: rows={row.n} fecha_cargue={row.loaded}")
spark.stop()

client = s3_client()
for bucket in BUCKETS:
    keys = list_keys(client, bucket, "")
    data_files = [k for k in keys if k.endswith(".parquet")]
    print(f"s3://{bucket}: {len(keys)} objects, {len(data_files)} parquet files")
