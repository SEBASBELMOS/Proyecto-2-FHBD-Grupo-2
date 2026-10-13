-- Trino: validation of the Gold replication against the Gold table written by the pipeline.
-- (1) row count + checksums (compare with validate_dremio.sql)
-- (2) exact diff in both directions: both EXCEPT queries must return 0 rows.
-- fecha_cargue is excluded on purpose (it only records when the pipeline ran).

WITH replica AS (
    SELECT
        u.id                                   AS user_id,
        p.post_year                            AS post_year,
        u.display_name                         AS display_name,
        u.reputation                           AS reputation,
        u.creation_year                        AS user_creation_year,
        COUNT_IF(p.post_type_id = 1)           AS questions,
        COUNT_IF(p.post_type_id = 2)           AS answers,
        COUNT(*)                               AS total_posts,
        SUM(p.score)                           AS total_score,
        ROUND(CAST(SUM(p.score) AS DECIMAL(38, 6)) / COUNT(p.score), 2) AS avg_score,
        MAX(p.score)                           AS max_score,
        SUM(p.view_count)                      AS total_views,
        MIN(p.creation_date)                   AS first_post_date,
        MAX(p.creation_date)                   AS last_post_date
    FROM iceberg.silver.post_hist p
    JOIN iceberg.silver.users_hist u ON p.owner_user_id = u.id
    GROUP BY u.id, p.post_year, u.display_name, u.reputation, u.creation_year
),
gold AS (
    SELECT user_id, post_year, display_name, reputation, user_creation_year, questions, answers,
           total_posts, total_score, avg_score, max_score, total_views, first_post_date, last_post_date
    FROM iceberg.gold.cant_post_x_user_hist
)
SELECT
    COUNT(*)                                       AS row_count,
    SUM(user_id)                                   AS sum_user_id,
    SUM(questions)                                 AS sum_questions,
    SUM(answers)                                   AS sum_answers,
    SUM(total_posts)                               AS sum_total_posts,
    SUM(total_score)                               AS sum_total_score,
    SUM(CAST(avg_score * 100 AS BIGINT))           AS sum_avg_score_x100
FROM replica;

-- Same checksums over the Gold table as read from Trino
WITH replica AS (
    SELECT
        u.id                                   AS user_id,
        p.post_year                            AS post_year,
        u.display_name                         AS display_name,
        u.reputation                           AS reputation,
        u.creation_year                        AS user_creation_year,
        COUNT_IF(p.post_type_id = 1)           AS questions,
        COUNT_IF(p.post_type_id = 2)           AS answers,
        COUNT(*)                               AS total_posts,
        SUM(p.score)                           AS total_score,
        ROUND(CAST(SUM(p.score) AS DECIMAL(38, 6)) / COUNT(p.score), 2) AS avg_score,
        MAX(p.score)                           AS max_score,
        SUM(p.view_count)                      AS total_views,
        MIN(p.creation_date)                   AS first_post_date,
        MAX(p.creation_date)                   AS last_post_date
    FROM iceberg.silver.post_hist p
    JOIN iceberg.silver.users_hist u ON p.owner_user_id = u.id
    GROUP BY u.id, p.post_year, u.display_name, u.reputation, u.creation_year
),
gold AS (
    SELECT user_id, post_year, display_name, reputation, user_creation_year, questions, answers,
           total_posts, total_score, avg_score, max_score, total_views, first_post_date, last_post_date
    FROM iceberg.gold.cant_post_x_user_hist
)
SELECT
    COUNT(*) AS row_count, SUM(user_id) AS sum_user_id, SUM(questions) AS sum_questions,
    SUM(answers) AS sum_answers, SUM(total_posts) AS sum_total_posts, SUM(total_score) AS sum_total_score,
    SUM(CAST(avg_score * 100 AS BIGINT)) AS sum_avg_score_x100
FROM gold;

-- replica EXCEPT gold  -> expected: 0 rows
WITH replica AS (
    SELECT
        u.id                                   AS user_id,
        p.post_year                            AS post_year,
        u.display_name                         AS display_name,
        u.reputation                           AS reputation,
        u.creation_year                        AS user_creation_year,
        COUNT_IF(p.post_type_id = 1)           AS questions,
        COUNT_IF(p.post_type_id = 2)           AS answers,
        COUNT(*)                               AS total_posts,
        SUM(p.score)                           AS total_score,
        ROUND(CAST(SUM(p.score) AS DECIMAL(38, 6)) / COUNT(p.score), 2) AS avg_score,
        MAX(p.score)                           AS max_score,
        SUM(p.view_count)                      AS total_views,
        MIN(p.creation_date)                   AS first_post_date,
        MAX(p.creation_date)                   AS last_post_date
    FROM iceberg.silver.post_hist p
    JOIN iceberg.silver.users_hist u ON p.owner_user_id = u.id
    GROUP BY u.id, p.post_year, u.display_name, u.reputation, u.creation_year
),
gold AS (
    SELECT user_id, post_year, display_name, reputation, user_creation_year, questions, answers,
           total_posts, total_score, avg_score, max_score, total_views, first_post_date, last_post_date
    FROM iceberg.gold.cant_post_x_user_hist
)
SELECT COUNT(*) AS rows_only_in_replica FROM (SELECT * FROM replica EXCEPT SELECT * FROM gold);

-- gold EXCEPT replica  -> expected: 0 rows
WITH replica AS (
    SELECT
        u.id                                   AS user_id,
        p.post_year                            AS post_year,
        u.display_name                         AS display_name,
        u.reputation                           AS reputation,
        u.creation_year                        AS user_creation_year,
        COUNT_IF(p.post_type_id = 1)           AS questions,
        COUNT_IF(p.post_type_id = 2)           AS answers,
        COUNT(*)                               AS total_posts,
        SUM(p.score)                           AS total_score,
        ROUND(CAST(SUM(p.score) AS DECIMAL(38, 6)) / COUNT(p.score), 2) AS avg_score,
        MAX(p.score)                           AS max_score,
        SUM(p.view_count)                      AS total_views,
        MIN(p.creation_date)                   AS first_post_date,
        MAX(p.creation_date)                   AS last_post_date
    FROM iceberg.silver.post_hist p
    JOIN iceberg.silver.users_hist u ON p.owner_user_id = u.id
    GROUP BY u.id, p.post_year, u.display_name, u.reputation, u.creation_year
),
gold AS (
    SELECT user_id, post_year, display_name, reputation, user_creation_year, questions, answers,
           total_posts, total_score, avg_score, max_score, total_views, first_post_date, last_post_date
    FROM iceberg.gold.cant_post_x_user_hist
)
SELECT COUNT(*) AS rows_only_in_gold FROM (SELECT * FROM gold EXCEPT SELECT * FROM replica);

-- First rows ordered by key (compare with the same query in Dremio)
SELECT user_id, post_year, questions, answers, total_posts, total_score, avg_score
FROM iceberg.gold.cant_post_x_user_hist
ORDER BY user_id, post_year
LIMIT 20;
