# %% [markdown]
# # prepare_bronze_manual
# Manual step (run ONCE before the pipeline). Streams the public ClickHouse StackOverflow
# parquet files, keeps only January-June of each year (raw columns, no transformations) and
# writes them to MinIO (Bronze, Parquet, overwritten on every run):
#
# - `bronze/post/2020/post_2020.parquet`   (Posts 2020-H1)
# - `bronze/post/2021/post_2021.parquet`   (Posts 2021-H1)
# - `bronze/users/2020/users_2020.parquet` (Users created in 2020-H1)
#
# `users_2021` is NOT written here: it is ingested by the pipeline (task `bronze_ingest`, dlt).

# %%
import sys
import time

sys.path.insert(0, "/opt/airflow/jobs")

import pyarrow.parquet as pq

from common import BRONZE_POSTS, BRONZE_USERS, ensure_buckets, iter_window_batches, minio_fs, open_source_parquet

TARGETS = [
    ("posts/2020.parquet", 2020, BRONZE_POSTS[2020]),
    ("posts/2021.parquet", 2021, BRONZE_POSTS[2021]),
    # Users is a single file: the window of 2020 is applied on CreationDate.
    ("users.parquet", 2020, BRONZE_USERS[2020]),
]

# %%
ensure_buckets()
fs = minio_fs()

for source_path, year, destination in TARGETS:
    started = time.time()
    schema = open_source_parquet(source_path).schema_arrow
    rows = 0
    with fs.open_output_stream(destination) as sink, pq.ParquetWriter(sink, schema, compression="snappy") as writer:
        for table in iter_window_batches(source_path, year):
            writer.write_table(table)
            rows += table.num_rows
    print(f"{source_path} -> s3://{destination}: {rows:,} rows in {time.time() - started:.0f}s")
