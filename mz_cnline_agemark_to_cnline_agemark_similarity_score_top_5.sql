
delete from tutorial.cnline_agemark_to_cnline_agemark_similarity_score_top_5;  -- for the subsequent update
insert into tutorial.cnline_agemark_to_cnline_agemark_similarity_score_top_5


WITH top_1 AS (
SELECT cnline_age1,
       cnline_age2          AS top_1_line,
       cosine_similarity
FROM spectrum.pa_II_ibcf_cnline_age_similarity
WHEre RANK = 1
 AND update_date = '2025-04-01' --- 取最新
),

top_2 AS (
SELECT cnline_age1,
       cnline_age2 AS top_2_line,
       cosine_similarity
FROM spectrum.pa_II_ibcf_cnline_age_similarity
WHEre RANK = 2
AND update_date = '2025-04-01' --- 取最新
),

top_3 AS (
SELECT cnline_age1,
       cnline_age2 AS top_3_line,
       cosine_similarity
FROM spectrum.pa_II_ibcf_cnline_age_similarity
WHEre RANK = 3
AND update_date = '2025-04-01' --- 取最新
),


top_4 AS (
SELECT cnline_age1,
       cnline_age2 AS top_4_line,
       cosine_similarity
FROM spectrum.pa_II_ibcf_cnline_age_similarity
WHEre RANK = 4
AND update_date = '2025-04-01' --- 取最新
),


top_5 AS (
SELECT cnline_age1,
       cnline_age2 AS top_5_line,
       cosine_similarity
FROM spectrum.pa_II_ibcf_cnline_age_similarity
WHEre RANK = 5
AND update_date = '2025-04-01' --- 取最新
),

transaction_cte AS (
SELECT cn_line AS cnline_age1,
       COUNT(DISTINCT parent_order_id) AS transactions
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
  AND extract('year' FROM DATE(date_id)) = extract('year' from current_date)
  GROUP BY 1
  )

SELECT top_1.cnline_age1,
       top_1_line,
       top_2_line,
       top_3_line,
       top_4_line,
       top_5_line
FROM top_1
LEFT JOIN top_2
        ON top_1.cnline_age1 = top_2.cnline_age1
LEFT JOIN top_3
        ON top_1.cnline_age1 = top_3.cnline_age1
LEFT JOIN top_4
        ON top_1.cnline_age1 = top_4.cnline_age1
LEFT JOIN top_5
        ON top_1.cnline_age1 = top_5.cnline_age1
LEFT JOIN transaction_cte
       ON top_1.cnline_age1  = transaction_cte.cnline_age1
ORDER BY COALESCE(transaction_cte.transactions,0) DESC;