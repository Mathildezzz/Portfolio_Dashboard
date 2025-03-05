DELETE FROM tutorial.mz_portfolio_cnline_price_range_distribution;
INSERT INTO tutorial.mz_portfolio_cnline_price_range_distribution

WITH product_cte AS (
SELECT product.lego_sku_id,
       product.cn_line,
       CASE WHEN product.rsp > 0 AND product.rsp < 300 THEN 'LPP'
            WHEN product.rsp >= 300 AND product.rsp < 800 THEN 'MPP'
            WHEN product.rsp >= 800 THEN 'HPP'
       END                                                                                AS product_rrp_price_range
      FROM edw.d_dl_product_info_latest product
      INNER JOIN (SELECT DISTINCT lego_sku_id 
                    FROM dm_view.offline_lcs_cs__by_sku_fnl 
                   WHERE extract('year' FROM DATE(date_id)) = extract('year' from current_date)     -- 看今年在卖的品
              ) trans
              ON product.lego_sku_id = trans.lego_sku_id
)

SELECT cn_line,
       product_rrp_price_range,
        COUNT(DISTINCT lego_sku_id) AS sku_count
 FROM product_cte
 GROUP BY 1,2;
 
 
 
