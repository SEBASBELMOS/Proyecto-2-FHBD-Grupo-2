-- Dremio: query the Gold table written by the pipeline (source `nessie`, Nessie catalog).
-- Run it from http://localhost:9047 (SQL Runner) or from DBeaver.

SELECT *
FROM nessie.gold.cant_post_x_user_hist
ORDER BY total_posts DESC, user_id
LIMIT 20;

SELECT post_year, COUNT(*) AS rows_per_year, SUM(total_posts) AS posts, MAX(fecha_cargue) AS fecha_cargue
FROM nessie.gold.cant_post_x_user_hist
GROUP BY post_year
ORDER BY post_year;
