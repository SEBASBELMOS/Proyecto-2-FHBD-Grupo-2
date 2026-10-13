# %% [markdown]
# # gold_agg  (pipeline task 3)
# Builds the Gold Iceberg table `nessie.gold.cant_post_x_user_hist`: one row per
# (user_id, post_year) joining Silver `post_hist` (owner_user_id) with Silver `users_hist` (id).
# Metrics: questions, answers, total posts, total/avg/max score, total views, first/last post.
# Loaded with `MERGE INTO` on (user_id, post_year); `fecha_cargue` is the load timestamp.
# The same aggregation can be rebuilt from Silver in Trino: see `sql/replicate_gold_trino.sql`.

# %%
import sys

sys.path.insert(0, "/opt/airflow/jobs")

from common import GOLD_LOCATION, GOLD_TABLE, POST_HIST, USERS_HIST, expire_old_snapshots, get_spark, require_tables

spark = get_spark("gold_agg")
require_tables(spark, POST_HIST, USERS_HIST)

# %% [markdown]
# ## Join + aggregate (must stay identical to sql/replicate_gold_trino.sql)

# %%
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW gold_source AS
    SELECT
        u.id                                                        AS user_id,
        p.post_year                                                 AS post_year,
        u.display_name                                              AS display_name,
        u.reputation                                                AS reputation,
        u.creation_year                                             AS user_creation_year,
        COUNT_IF(p.post_type_id = 1)                                AS questions,
        COUNT_IF(p.post_type_id = 2)                                AS answers,
        COUNT(*)                                                    AS total_posts,
        SUM(p.score)                                                AS total_score,
        ROUND(CAST(SUM(p.score) AS DECIMAL(38, 6)) / COUNT(p.score), 2) AS avg_score,
        MAX(p.score)                                                AS max_score,
        SUM(p.view_count)                                           AS total_views,
        MIN(p.creation_date)                                        AS first_post_date,
        MAX(p.creation_date)                                        AS last_post_date,
        current_timestamp()                                         AS fecha_cargue
    FROM {POST_HIST} p
    JOIN {USERS_HIST} u ON p.owner_user_id = u.id
    GROUP BY u.id, p.post_year, u.display_name, u.reputation, u.creation_year
""")

# %% [markdown]
# ## Create (if missing) and MERGE into Gold

# %%
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
        user_id             BIGINT,
        post_year           INT,
        display_name        STRING,
        reputation          BIGINT,
        user_creation_year  INT,
        questions           BIGINT,
        answers             BIGINT,
        total_posts         BIGINT,
        total_score         BIGINT,
        avg_score           DECIMAL(18, 2),
        max_score           INT,
        total_views         BIGINT,
        first_post_date     TIMESTAMP,
        last_post_date      TIMESTAMP,
        fecha_cargue        TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (post_year)
    LOCATION '{GOLD_LOCATION}'
    TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
""")

spark.sql(f"""
    MERGE INTO {GOLD_TABLE} t
    USING gold_source s
    ON t.user_id = s.user_id AND t.post_year = s.post_year
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
expire_old_snapshots(spark, GOLD_TABLE)

# %%
spark.sql(f"SELECT post_year, COUNT(*) AS rows, SUM(total_posts) AS posts, MAX(fecha_cargue) AS fecha_cargue FROM {GOLD_TABLE} GROUP BY post_year ORDER BY 1").show()
spark.sql(f"SELECT * FROM {GOLD_TABLE} ORDER BY total_posts DESC, user_id LIMIT 10").show(truncate=False)
spark.stop()
