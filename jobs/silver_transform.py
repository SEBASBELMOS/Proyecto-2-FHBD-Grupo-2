# %% [markdown]
# # silver_transform  (pipeline task 2)
# Builds the Silver Iceberg table `nessie.silver.users_hist` from the Bronze users files
# (`users_2020` prepared manually + `users_2021` ingested by dlt in task 1).
# Curated (heavy `AboutMe` dropped, typed columns), de-duplicated by user Id and loaded with
# `MERGE INTO`. `fecha_cargue` is the load timestamp. Re-running never duplicates rows.

# %%
import sys

sys.path.insert(0, "/opt/airflow/jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import BRONZE_USERS, USERS_HIST, USERS_HIST_LOCATION, expire_old_snapshots, get_spark

spark = get_spark("silver_transform")

# %% [markdown]
# ## Read Bronze (raw) and normalize column names
# dlt and the manual script may name columns differently; both are reduced to lowercase
# without underscores before selecting.

# %%
raw = spark.read.parquet(*[f"s3a://{p}" for p in BRONZE_USERS.values()])
raw = raw.toDF(*[c.replace("_", "").lower() for c in raw.columns])
raw.createOrReplaceTempView("bronze_users")

# %% [markdown]
# ## Curate and de-duplicate

# %%
curated = spark.sql("""
    SELECT
        CAST(id AS BIGINT)                                  AS id,
        CAST(CAST(reputation AS STRING) AS BIGINT)          AS reputation,
        CAST(creationdate AS TIMESTAMP)                     AS creation_date,
        CAST(displayname AS STRING)                         AS display_name,
        CAST(lastaccessdate AS TIMESTAMP)                   AS last_access_date,
        CAST(views AS INT)                                  AS views,
        CAST(upvotes AS INT)                                AS up_votes,
        CAST(downvotes AS INT)                              AS down_votes,
        NULLIF(CAST(websiteurl AS STRING), '')              AS website_url,
        NULLIF(CAST(location AS STRING), '')                AS location,
        CAST(accountid AS BIGINT)                           AS account_id,
        YEAR(CAST(creationdate AS TIMESTAMP))               AS creation_year
    FROM bronze_users
    WHERE id IS NOT NULL
""")

latest_first = Window.partitionBy("id").orderBy(F.col("last_access_date").desc_nulls_last())
source = (
    curated.withColumn("rn", F.row_number().over(latest_first))
    .filter("rn = 1")
    .drop("rn")
    .withColumn("fecha_cargue", F.current_timestamp())
)
source.createOrReplaceTempView("users_source")

# %% [markdown]
# ## Create (if missing) and MERGE into Silver

# %%
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {USERS_HIST} (
        id                BIGINT,
        reputation        BIGINT,
        creation_date     TIMESTAMP,
        display_name      STRING,
        last_access_date  TIMESTAMP,
        views             INT,
        up_votes          INT,
        down_votes        INT,
        website_url       STRING,
        location          STRING,
        account_id        BIGINT,
        creation_year     INT,
        fecha_cargue      TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (creation_year)
    LOCATION '{USERS_HIST_LOCATION}'
    TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
""")

spark.sql(f"""
    MERGE INTO {USERS_HIST} t
    USING users_source s
    ON t.id = s.id AND t.creation_year = s.creation_year
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
expire_old_snapshots(spark, USERS_HIST)

# %%
spark.sql(f"SELECT creation_year, COUNT(*) AS users, MAX(fecha_cargue) AS fecha_cargue FROM {USERS_HIST} GROUP BY creation_year ORDER BY 1").show()
spark.stop()
