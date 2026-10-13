# %% [markdown]
# # silver_post_hist_manual
# Manual step (run ONCE before the pipeline). Reads the two Bronze post parquet files and
# MERGEs them into the Silver Iceberg table `nessie.silver.post_hist`:
# curated (heavy `Body` dropped, sentinel values -> NULL, typed columns), de-duplicated by post
# Id and loaded with `MERGE INTO` (never overwrite). `fecha_cargue` is the load timestamp.

# %%
import sys

sys.path.insert(0, "/opt/airflow/jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import BRONZE_POSTS, POST_HIST, POST_HIST_LOCATION, expire_old_snapshots, get_spark

spark = get_spark("silver_post_hist_manual")

# %% [markdown]
# ## Read Bronze (raw) and normalize column names

# %%
paths = [f"s3a://{p}" for p in BRONZE_POSTS.values()]
raw = spark.read.parquet(*paths)
raw = raw.toDF(*[c.replace("_", "").lower() for c in raw.columns])
raw.createOrReplaceTempView("bronze_posts")

# %% [markdown]
# ## Curate: drop Body, cast types, sentinels (0 / epoch / '') -> NULL, dedupe by Id

# %%
EPOCH = "TIMESTAMP '1970-01-01 00:00:00'"
curated = spark.sql(f"""
    SELECT
        CAST(id AS BIGINT)                                          AS id,
        CAST(posttypeid AS INT)                                     AS post_type_id,
        NULLIF(CAST(acceptedanswerid AS BIGINT), 0)                 AS accepted_answer_id,
        NULLIF(CAST(CAST(parentid AS STRING) AS BIGINT), 0)         AS parent_id,
        NULLIF(CAST(owneruserid AS BIGINT), 0)                      AS owner_user_id,
        CAST(creationdate AS TIMESTAMP)                             AS creation_date,
        NULLIF(CAST(lasteditdate AS TIMESTAMP), {EPOCH})            AS last_edit_date,
        NULLIF(CAST(lastactivitydate AS TIMESTAMP), {EPOCH})        AS last_activity_date,
        NULLIF(CAST(closeddate AS TIMESTAMP), {EPOCH})              AS closed_date,
        CAST(score AS INT)                                          AS score,
        CAST(viewcount AS INT)                                      AS view_count,
        CAST(answercount AS INT)                                    AS answer_count,
        CAST(commentcount AS INT)                                   AS comment_count,
        CAST(title AS STRING)                                       AS title,
        CAST(tags AS STRING)                                        AS tags,
        CAST(contentlicense AS STRING)                              AS content_license,
        YEAR(CAST(creationdate AS TIMESTAMP))                       AS post_year
    FROM bronze_posts
    WHERE id IS NOT NULL
""")

latest_first = Window.partitionBy("id").orderBy(F.col("last_activity_date").desc_nulls_last())
source = (
    curated.withColumn("rn", F.row_number().over(latest_first))
    .filter("rn = 1")
    .drop("rn")
    .withColumn("fecha_cargue", F.current_timestamp())
)
source.createOrReplaceTempView("post_source")

# %% [markdown]
# ## Create (if missing) and MERGE into Silver

# %%
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {POST_HIST} (
        id                  BIGINT,
        post_type_id        INT,
        accepted_answer_id  BIGINT,
        parent_id           BIGINT,
        owner_user_id       BIGINT,
        creation_date       TIMESTAMP,
        last_edit_date      TIMESTAMP,
        last_activity_date  TIMESTAMP,
        closed_date         TIMESTAMP,
        score               INT,
        view_count          INT,
        answer_count        INT,
        comment_count       INT,
        title               STRING,
        tags                STRING,
        content_license     STRING,
        post_year           INT,
        fecha_cargue        TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (post_year)
    LOCATION '{POST_HIST_LOCATION}'
    TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
""")

spark.sql(f"""
    MERGE INTO {POST_HIST} t
    USING post_source s
    ON t.id = s.id AND t.post_year = s.post_year
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
expire_old_snapshots(spark, POST_HIST)

# %%
spark.sql(f"SELECT post_year, COUNT(*) AS posts, MAX(fecha_cargue) AS fecha_cargue FROM {POST_HIST} GROUP BY post_year ORDER BY 1").show()
spark.stop()
