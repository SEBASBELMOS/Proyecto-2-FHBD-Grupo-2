# %% [markdown]
# # bronze_ingest  (pipeline task 1)
# Ingests StackOverflow `users` for 2021-H1 with **dlt** (data load tool, dlthub):
# reads `users.parquet` from the public source URL in streaming batches, keeps users created
# between 2021-01-01 and 2021-06-30 and writes Parquet to MinIO:
# `s3://bronze/users/2021/users_2021.parquet`.
#
# `write_disposition="replace"` makes every run overwrite the previous file (no accumulation).

# %%
import os
import sys

sys.path.insert(0, "/opt/airflow/jobs")

# Keep the raw column names (CreationDate, DisplayName, ...): Bronze is raw data.
os.environ["SCHEMA__NAMING"] = "direct"

import dlt
from dlt.sources.credentials import AwsCredentials

from common import (
    AWS_KEY,
    AWS_REGION,
    AWS_SECRET,
    BRONZE_BUCKET,
    BRONZE_USERS,
    MINIO_ENDPOINT,
    USERS_SOURCE_URL,
    ensure_buckets,
    iter_window_batches,
    list_keys,
    s3_client,
)

YEAR = 2021
FINAL_KEY = BRONZE_USERS[YEAR].split("/", 1)[1]  # users/2021/users_2021.parquet

# %% [markdown]
# ## dlt resource: streamed + filtered source

# %%
@dlt.resource(name="users_2021", write_disposition="replace")
def users_2021():
    print(f"Reading {USERS_SOURCE_URL} (window {YEAR}-01-01 .. {YEAR}-06-30)")
    for table in iter_window_batches("users.parquet", YEAR):
        yield table


# %% [markdown]
# ## dlt pipeline -> MinIO (filesystem destination, parquet)
# The layout makes dlt write `<dataset_name>/2021/users_2021.parquet` = `users/2021/users_2021.parquet`.

# %%
ensure_buckets()

destination = dlt.destinations.filesystem(
    bucket_url=f"s3://{BRONZE_BUCKET}",
    credentials=AwsCredentials(
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        endpoint_url=MINIO_ENDPOINT,
        region_name=AWS_REGION,
    ),
    layout="2021/{table_name}.{ext}",
    warn_unsafe_layout_separators=False,  # single table, no prefix collisions possible
)
pipeline = dlt.pipeline(
    pipeline_name="bronze_users_2021",
    destination=destination,
    dataset_name="users",
    pipelines_dir="/tmp/dlt_pipelines",
)
load_info = pipeline.run(users_2021(), loader_file_format="parquet")
print(load_info)

# %% [markdown]
# ## Clean dlt bookkeeping and verify: exactly one file, at the expected path
# dlt writes its own state tables (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`, `init`)
# next to the data. They are not needed for `replace` loads, so they are removed to keep
# Bronze limited to the data files.

# %%
client = s3_client()
for key in list_keys(client, BRONZE_BUCKET, "users/"):
    if key.split("/")[1].startswith("_dlt_") or key == "users/init":
        client.delete_object(Bucket=BRONZE_BUCKET, Key=key)
keys = list_keys(client, BRONZE_BUCKET, "users/2021/")
print("Objects under bronze/users/2021/:", keys)
if keys != [FINAL_KEY]:
    raise RuntimeError(f"Expected exactly [{FINAL_KEY}] but found {keys}")
size_mb = client.head_object(Bucket=BRONZE_BUCKET, Key=FINAL_KEY)["ContentLength"] / 1e6
print(f"s3://{BRONZE_USERS[YEAR]} written ({size_mb:.1f} MB)")
