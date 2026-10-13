-- Dremio: row count + aggregate checksum of the Gold table (compare with validate_trino.sql).
SELECT
    COUNT(*)                                       AS row_count,
    SUM(user_id)                                   AS sum_user_id,
    SUM(questions)                                 AS sum_questions,
    SUM(answers)                                   AS sum_answers,
    SUM(total_posts)                               AS sum_total_posts,
    SUM(total_score)                               AS sum_total_score,
    SUM(CAST(avg_score * 100 AS BIGINT))           AS sum_avg_score_x100
FROM nessie.gold.cant_post_x_user_hist;

-- First rows ordered by key (compare with the same query in Trino)
SELECT user_id, post_year, questions, answers, total_posts, total_score, avg_score
FROM nessie.gold.cant_post_x_user_hist
ORDER BY user_id, post_year
LIMIT 20;
