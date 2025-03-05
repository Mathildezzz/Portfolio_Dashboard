DELETE FRom tutorial.mz_portfolio_cnline_agemark_distribution;
INSERT INTO tutorial.mz_portfolio_cnline_agemark_distribution

WITH product_cte AS (
SELECT product.lego_sku_id,
          product.cn_line,
          COALESCE(
                      -- Convert fraction '1 1/2' to a decimal value like '1.5'
                      CASE 
                          WHEN TRIM(age_mark) ~ '^[0-9]+ [0-9]+/[0-9]+' THEN
                              CAST(SPLIT_PART(TRIM(age_mark), ' ', 1) AS INT) + 
                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 1) AS INT) / 
                              CAST(SPLIT_PART(SPLIT_PART(TRIM(age_mark), ' ', 2), '/', 2) AS INT)
                          -- Remove non-numeric characters after leading digits (like '+', '-' etc.)
                          ELSE CAST(REGEXP_REPLACE(TRIM(age_mark), '[^0-9]+.*', '') AS INT)
                      END, 
                      0) AS min_age_mark 
      FROM edw.d_dl_product_info_latest product
      INNER JOIN (SELECT DISTINCT lego_sku_id 
                    FROM dm_view.offline_lcs_cs__by_sku_fnl 
                   WHERE extract('year' FROM DATE(date_id)) = extract('year' from current_date)     -- 看今年在卖的品
              ) trans
              ON product.lego_sku_id = trans.lego_sku_id
      WHERE TRIM(age_mark) ~ '^[0-9]+([ ]*[0-9]*/[0-9]+)?'   -- Filter out rows that don't start with a number or a fraction like '1 1/2'
)

SELECT cn_line,
       CASE WHEN min_age_mark <=5 THEN '0-5'
           WHEN min_age_mark >=6 and min_age_mark <= 8 THEN '6-8'
           WHEN min_age_mark >= 9 and min_age_mark <= 12 THEN '9-12'
           WHEN min_age_mark >= 13 and min_age_mark <= 17 THEN '13-17'
           WHEN min_age_mark >= 18 then '18+'
        END age_mark_min_group,
        COUNT(DISTINCT lego_sku_id) AS sku_count
 FROM product_cte
 GROUP BY 1,2;
 
 
 
