-- Trino: rebuilds the Gold table `cant_post_x_user_hist` from the two Silver tables.
-- Catalog `iceberg` (Nessie + MinIO), see trino/catalog/iceberg.properties.
-- Same join, grouping and expressions as jobs/gold_agg.py (without fecha_cargue).

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
ORDER BY user_id, post_year;
