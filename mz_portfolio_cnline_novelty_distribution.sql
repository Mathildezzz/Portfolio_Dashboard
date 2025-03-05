DELETE FRom tutorial.mz_portfolio_cnline_novelty_distribution;
INSERT INTO tutorial.mz_portfolio_cnline_novelty_distribution

WITH product_cte AS (
SELECT product.lego_sku_id,
       product.cn_line,
       CASE WHEN extract('year' FROM bu_cn_launch_date) = extract('year' from current_date) THEN 'novelty' ELSE 'existing' END       AS if_novelty
      FROM edw.d_dl_product_info_latest product
      INNER JOIN (SELECT DISTINCT lego_sku_id 
                    FROM dm_view.offline_lcs_cs__by_sku_fnl 
                   WHERE extract('year' FROM DATE(date_id)) = extract('year' from current_date)    -- 看今年在卖的品
              ) trans
              ON product.lego_sku_id = trans.lego_sku_id
      WHERE TRIM(age_mark) ~ '^[0-9]+([ ]*[0-9]*/[0-9]+)?'   -- Filter out rows that don't start with a number or a fraction like '1 1/2'
)

SELECT cn_line,
       if_novelty,
       COUNT(DISTINCT lego_sku_id) AS sku_count
 FROM product_cte
 GROUP BY 1,2;