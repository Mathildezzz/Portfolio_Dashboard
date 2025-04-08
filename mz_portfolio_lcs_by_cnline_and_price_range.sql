-- DROP TABLE IF EXiSTS tutorial.mz_portfolio_lcs_by_cnline_and_price_range;
-- CREATE TABLE tutorial.mz_portfolio_lcs_by_cnline_and_price_range AS

delete from tutorial.mz_portfolio_lcs_by_cnline_and_price_range;  -- for the subsequent update
insert into tutorial.mz_portfolio_lcs_by_cnline_and_price_range

WITH product_cte AS (
SELECT 
        lego_sku_id,
        cn_line,
          CASE WHEN product.rsp > 0 AND product.rsp < 300 THEN 'LPP'
            WHEN product.rsp >= 300 AND product.rsp < 800 THEN 'MPP'
            WHEN product.rsp >= 800 THEN 'HPP'
          END                                                                                AS product_rrp_price_range
      FROM edw.d_dl_product_info_latest product
),

sales_plan_cte AS (
SELECT plan.lego_sku_id,
       product.lego_sku_name_cn,
       lego_year                 AS plan_year,
       lego_month                AS plan_month,
       product.cn_line           AS cn_line,   
       product.cn_lcs_launch_date,
       product_cte.product_rrp_price_range,
       product.rsp,
       SUM(sku_plan_qty) AS plan_qty
    FROM mg.sku_plan_monthly plan
LEFT JOIN edw.d_dl_product_info_latest product
       ON plan.lego_sku_id = product.lego_sku_id
LEFT JOIN product_cte
       ON plan.lego_sku_id = product_cte.lego_sku_id
WHERE lego_year = extract('year' from current_date)
  AND plan_month <= extract('month' FROM current_date)
AND review_channel_dp LIKE '%LCS%' 
AND dp_version = 'DP03'
GROUP BY 1,2,3,4,5,6,7,8
),

sales_plan_table AS (
SELECT sales_plan_cte.cn_line,
       sales_plan_cte.product_rrp_price_range,
       SUM(rsp*plan_qty)                                                             AS sales_plan
 FROM sales_plan_cte
 GROUP BY 1,2
 UNION ALL
 SELECT 
       'TTL'             AS cn_line,
       'TTL'             AS product_rrp_price_range,
       SUM(rsp*plan_qty) AS sales_plan
 FROM sales_plan_cte
 GROUP BY 1,2
 ),
    
    
ttl_sales_TY AS (
SELECT product_cte.cn_line,
       product_cte.product_rrp_price_range,
       COUNT(DISTINCT parent_order_id)                                                                                               AS transactions,
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
LEFT JOIN product_cte
       ON sales.lego_sku_id = product_cte.lego_sku_id   
WHERE 1 = 1 
  AND DATE(date_id) >= '2024-12-30'
  GROUP BY 1,2
 UNION ALL
 SELECT
       'TTL' AS cn_line,
       'TTL' AS product_rrp_price_range,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
  AND DATE(date_id) >= '2024-12-30'
  GROUP BY 1,2
  ),
  
ttl_sales_LY AS (
SELECT product_cte.cn_line,
       product_cte.product_rrp_price_range,
       COUNT(DISTINCT parent_order_id)                                                                                               AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
LEFT JOIN product_cte
       ON sales.lego_sku_id = product_cte.lego_sku_id   
WHERE 1 = 1 
   AND date_id >= '2024-01-01' --- LY YTD
   AND date_id <= (current_date - interval '1 year' + interval '2 days')::date
   GROUP BY 1,2
   UNION ALL
SELECT 'TTL' AS cn_line,
       'TTL' AS product_rrp_price_range,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
   AND date_id >= '2024-01-01' --- LY YTD
   AND date_id <= (current_date - interval '1 year' + interval '2 days')::date
   GROUP BY 1,2
  ),
  
sales AS (  
SELECT ttl_sales_TY.cn_line,
       ttl_sales_TY.product_rrp_price_range,
       ttl_sales_TY.sales_rrp,
       sales_plan_table.sales_plan,
       ttl_sales_TY.sales_rrp/NULLIF(sales_plan_table.sales_plan,0)                                AS sales_progress,
       
       ttl_sales_TY.transactions,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_TY.transactions,0)                   AS atv,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/(SELECT SUM(sales_rrp) FROM ttl_sales_TY  WHERE product_rrp_price_range <> 'TTL')                AS sales_rrp_share,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_LY.sales_rrp,0) - 1                                                                       AS sales_vs_LY,
       CAST(ttl_sales_TY.transactions AS FLOAT)/NULLIF(ttl_sales_LY.transactions,0) - 1                                                                 AS transactions_vs_LY,
       NULLIF((CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_TY.transactions,0))/NULLIF((CAST(ttl_sales_LY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_LY.transactions,0)),0) - 1,0) AS atv_vs_LY
FROM ttl_sales_TY
LEFT JOIN ttl_sales_LY
       ON ttl_sales_TY.product_rrp_price_range = ttl_sales_LY.product_rrp_price_range
      AND ttl_sales_TY.cn_line = ttl_sales_LY.cn_line
LEFT JOIN sales_plan_table
       ON ttl_sales_TY.product_rrp_price_range = sales_plan_table.product_rrp_price_range
      AND ttl_sales_TY.cn_line = sales_plan_table.cn_line      
),



omni_trans_fact as
    ( 
        select
        order_paid_time,
        date(tr.order_paid_date) as order_paid_date,
        kyid,
        case
        when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar))
        else null end as omni_channel_member_id, -- 优先取member_detail_id，缺失情况下再取渠道内部id
        cast(mbr.id as varchar) AS member_detail_id,
        tr.parent_order_id,
        tr.lego_sku_id,
        -----------------------------
       tr.cn_line,
       product_cte.product_rrp_price_range,
    --   CASE WHEN tr.cn_line <> 'LEL' THEN tr.cn_line
    --         WHEN tr.cn_line = 'LEL' and product.lego_sku_name_cn LIKE '%钥匙%' THEN 'LEL_KEYCHAINS'
    --         WHEN tr.cn_line = 'LEL' and product.lego_sku_name_cn NOT LIKE '%钥匙%' THEN 'LEL_NON_KEYCHAINS'
    --   END                             AS cn_line,
    
       CASE WHEN tr.city_tier IS NULL THEN 'unspecified' ELSE tr.city_tier END                     AS city_tier,
       CASE WHEN ps.city_maturity_type IS NULL THEN '4_unspecified' ELSE ps.city_maturity_type END AS city_maturity_type,
        --------------------------
        tr.sales_qty, -- 用于为LCS判断正负单
        tr.if_eff_order_tag, -- 该字段仅对LCS有true / false之分，对于其余渠道均为true
        tr.is_member_order,
        tr.order_rrp_amt
    FROM edw.f_omni_channel_order_detail as tr
LEFT JOIN edw.f_crm_member_detail as mbr
       on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
LEFT JOIN product_cte
       ON tr.lego_sku_id = product_cte.lego_sku_id
LEFT JOIN (
                SELECT DISTINCT store.city_cn,city_maturity.city_type AS city_maturity_type
                FROM  edw.d_dl_phy_store store
                LEFT JOIN tutorial.mkt_city_type_roy_v1 city_maturity  
                      ON city_maturity.city_chn = store.city_cn
              ) as ps 
   ON tr.city_cn = ps.city_cn
    WHERE 1 = 1
      and source_channel in ('LCS')
      and date(tr.order_paid_date) < current_date
      and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
    ),
    
new_member_ty AS (
  SELECT DISTINCT member_detail_id
     FROM edw.d_member_detail
     WHERE  1= 1
      AND DATE(join_time) >= '2024-12-30'
      AND DATE(join_time) < current_date  --- TY YTD
      AND eff_reg_channel LIKE '%LCS%'
  ),
  
 new_member_ly AS (
  SELECT DISTINCT member_detail_id
     FROM edw.d_member_detail
     WHERE  1= 1
      AND DATE(join_time) >= '2024-01-01' --- LY YTD
      AND DATE(join_time) <= (current_date - interval '1 year' + interval '2 days')::date
      AND eff_reg_channel LIKE '%LCS%'
  ),
  
  ------------------- lifetime purchase ranking -----------------------------
 purchase_order_rk AS (
 SELECT *, 
       ROW_NUMBER () OVER (PARTITION BY omni_channel_member_id ORDER BY order_paid_time ASC) AS rk
  FROM
     (
        SELECT DISTINCT parent_order_id, order_paid_time, omni_channel_member_id
         FROM omni_trans_fact
        WHERE if_eff_order_tag = TRUE
          AND is_member_order = TRUE
     )
 ),
 
member_KPI_TY AS (
  SELECT  trans.product_rrp_price_range,
          trans.cn_line,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)             AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                     AS member_frequency,
          
        ----------- new ------------
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS new_member_shopper,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS new_member_shopper_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                            AS new_member_sales,                 
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_member_sales_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)                AS new_member_atv_buying_the_line,
      
         ----------- existing 0-1
          CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)      AS existing_0_1_member_sales,
          CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_0_1_member_sales_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk = 1) then trans.parent_order_id else null end),0)                AS existing_0_1_member_atv_buying_the_line,
      
         ----------- existing repurchase
         
         CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS existing_repurchase_member_shopper,
         CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS existing_repurchase_member_shopper_share,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)       AS existing_repurchase_member_sales,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_repurchase_member_sales_share,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk >=2 ) then trans.parent_order_id else null end),0)                AS existing_repurchase_member_atv_buying_the_line,
      
             ----------------
        --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_member_shopper_share,
        
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_repurchase_member_shopper,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share
      
    from omni_trans_fact trans
 LEFT JOIN new_member_ty
        ON trans.member_detail_id::integer = new_member_ty.member_detail_id::integer
 LEFT JOIN purchase_order_rk
            ON trans.parent_order_id = purchase_order_rk.parent_order_id
 where 1 = 1
   and DATE(order_paid_date) >= '2024-12-30'
GROUP BY 1,2
UNION ALL

 SELECT 'TTL' AS product_rrp_price_range,
        'TTL' AS cn_line,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0) AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                         AS member_frequency,
         
          ----------- new ------------
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS new_member_shopper,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS new_member_shopper_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                            AS new_member_sales,                 
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_member_sales_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)                AS new_member_atv_buying_the_line,
      
         ----------- existing 0-1
          CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)      AS existing_0_1_member_sales,
          CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_0_1_member_sales_share,
          CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk = 1) then trans.parent_order_id else null end),0)                AS existing_0_1_member_atv_buying_the_line,
      
         ----------- existing repurchase
         
         CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS existing_repurchase_member_shopper,
         CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS existing_repurchase_member_shopper_share,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)       AS existing_repurchase_member_sales,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_repurchase_member_sales_share,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk >=2 ) then trans.parent_order_id else null end),0)                AS existing_repurchase_member_atv_buying_the_line,
      
             ----------------
        --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_member_shopper_share,
        
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_repurchase_member_shopper,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share
      
   from omni_trans_fact trans
 LEFT JOIN new_member_ty
        ON trans.member_detail_id::integer = new_member_ty.member_detail_id::integer
 LEFT JOIN purchase_order_rk
            ON trans.parent_order_id = purchase_order_rk.parent_order_id
 where 1 = 1
   and DATE(order_paid_date) >= '2024-12-30' 
    ),
    
member_KPI_LY AS (
  SELECT  trans.product_rrp_price_range,
          trans.cn_line,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)             AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                     AS member_frequency,
       
           ----------- new ------------
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS new_member_shopper,
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS new_member_shopper_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                            AS new_member_sales,                 
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_member_sales_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)                AS new_member_atv_buying_the_line,
      
         ----------- existing 0-1
          CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)      AS existing_0_1_member_sales,
          CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_0_1_member_sales_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk = 1) then trans.parent_order_id else null end),0)                AS existing_0_1_member_atv_buying_the_line,
      
         ----------- existing repurchase
         
         CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS existing_repurchase_member_shopper,
         CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS existing_repurchase_member_shopper_share,
         CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)       AS existing_repurchase_member_sales,
         CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_repurchase_member_sales_share,
         CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk >=2 ) then trans.parent_order_id else null end),0)                AS existing_repurchase_member_atv_buying_the_line,
      
         
         
        ----------------
        --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_member_shopper_share,
        
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_repurchase_member_shopper,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share
      
       from omni_trans_fact trans
 LEFT JOIN new_member_ly
        ON trans.member_detail_id::integer = new_member_ly.member_detail_id::integer
 LEFT JOIN purchase_order_rk
            ON trans.parent_order_id = purchase_order_rk.parent_order_id
 where 1 = 1
      AND DATE(order_paid_date) >= '2024-01-01' --- LY YTD
      AND DATE(order_paid_date) <= (current_date - interval '1 year' + interval '2 days')::date
GROUP BY 1,2
UNION ALL

 SELECT 'TTL' AS product_rrp_price_range,
        'TTL' AS cn_line,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)             AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                     AS member_frequency,
          
           ----------- new ------------
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                        AS new_member_shopper,
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)    AS new_member_shopper_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                            AS new_member_sales,                 
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_member_sales_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)                AS new_member_atv_buying_the_line,
      
         ----------- existing 0-1
          CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                AS existing_0_1_member_shopper,
          CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk = 1) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)           AS existing_0_1_member_shopper_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)      AS existing_0_1_member_sales,
          CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_0_1_member_sales_share,
          CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk = 1) then trans.parent_order_id else null end),0)                AS existing_0_1_member_atv_buying_the_line,
      
         ----------- existing repurchase
         
         CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS existing_repurchase_member_shopper,
         CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS existing_repurchase_member_shopper_share,
         CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)       AS existing_repurchase_member_sales,
         CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_repurchase_member_sales_share,
         CAST((sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk >=2 ) then trans.parent_order_id else null end),0)                AS existing_repurchase_member_atv_buying_the_line,
      
         
         
        ----------------
        --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_member_shopper_share,
        
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_repurchase_member_shopper,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share
      
  from omni_trans_fact trans
 LEFT JOIN new_member_ly
        ON trans.member_detail_id::integer = new_member_ly.member_detail_id::integer
 LEFT JOIN purchase_order_rk
            ON trans.parent_order_id = purchase_order_rk.parent_order_id
 where 1 = 1
      AND DATE(order_paid_date) >= '2024-01-01' --- LY YTD
      AND DATE(order_paid_date) <= (current_date - interval '1 year' + interval '2 days')::date
    )
    
SELECT sales.*,
           member_KPI_TY.member_shopper,
      CAST(member_KPI_TY.member_shopper AS FLOAT)/NULLIF(member_KPI_LY.member_shopper,0) - 1 AS member_shopper_vs_LY,
      member_KPI_TY.member_sales,
      member_KPI_TY.member_sales/NULLIF( member_KPI_LY.member_sales,0) - 1     AS member_sales_vs_LY,
      member_KPI_TY.member_atv,
      member_KPI_TY.member_atv/NULLIF( member_KPI_LY.member_atv,0) - 1         AS member_atv_vs_LY,
      member_KPI_TY.member_frequency,
      member_KPI_TY.member_frequency/NULLIF( member_KPI_LY.member_frequency,0) - 1 AS member_frequency_vs_LY,
      
      
      
       --- new
      member_KPI_TY.new_member_shopper,
      CAST(member_KPI_TY.new_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.new_member_shopper,0) - 1           AS new_member_shopper_vs_LY,
      member_KPI_TY.new_member_shopper_share,
      member_KPI_TY.new_member_shopper_share - member_KPI_LY.new_member_shopper_share                          AS new_member_shopper_share_vs_LY,
      member_KPI_TY.new_member_sales,
      CAST(member_KPI_TY.new_member_sales AS FLOAT)/NULLIF(member_KPI_LY.new_member_sales,0) - 1               AS new_member_sales_vs_LY,
      member_KPI_TY.new_member_sales_share,
      member_KPI_TY.new_member_sales_share - member_KPI_LY.new_member_sales_share                              AS new_member_sales_share_vs_LY,
      member_KPI_TY.new_member_atv_buying_the_line,
      CAST(member_KPI_TY.new_member_atv_buying_the_line AS FLOAT)/NULLIF(member_KPI_LY.new_member_atv_buying_the_line,0) - 1   AS new_member_atv_buying_the_line_vs_LY,       
      
     
      ----------- existing 0-1
      member_KPI_TY.existing_0_1_member_shopper,
      CAST(member_KPI_TY.existing_0_1_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.existing_0_1_member_shopper,0) - 1                                        AS existing_0_1_member_shopper_vs_LY,
      member_KPI_TY.existing_0_1_member_shopper_share,
      member_KPI_TY.existing_0_1_member_shopper_share -  member_KPI_LY.existing_0_1_member_shopper_share                                                      AS existing_0_1_member_shopper_share_vs_LY,
      member_KPI_TY.existing_0_1_member_sales,
      CAST(member_KPI_TY.existing_0_1_member_sales AS FLOAT)/NULLIF(member_KPI_LY.existing_0_1_member_sales,0) - 1                                            AS existing_0_1_member_sales_vs_LY,
      member_KPI_TY.existing_0_1_member_sales_share,
      member_KPI_TY.existing_0_1_member_sales_share - member_KPI_LY.existing_0_1_member_sales_share                                                           AS existing_0_1_member_sales_share_vs_LY,
      member_KPI_TY.existing_0_1_member_atv_buying_the_line,
      CAST(member_KPI_TY.existing_0_1_member_atv_buying_the_line AS FLOAT)/NULLIF(member_KPI_LY.existing_0_1_member_atv_buying_the_line,0) - 1                AS existing_0_1_member_atv_buying_the_line_vs_LY,  
      
      
      ----------- existing repurchase
      member_KPI_TY.existing_repurchase_member_shopper,
      CAST(member_KPI_TY.existing_repurchase_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.existing_repurchase_member_shopper,0) - 1                           AS existing_repurchase_member_shopper_vs_LY,
      member_KPI_TY.existing_repurchase_member_shopper_share,
      member_KPI_TY.existing_repurchase_member_shopper_share -  member_KPI_LY.existing_repurchase_member_shopper_share                                         AS existing_repurchase_member_shopper_share_vs_LY,
      member_KPI_TY.existing_repurchase_member_sales,
      CAST(member_KPI_TY.existing_repurchase_member_sales AS FLOAT)/NULLIF(member_KPI_LY.existing_repurchase_member_sales,0) - 1                               AS existing_repurchase_member_sales_vs_LY,
      member_KPI_TY.existing_repurchase_member_sales_share,
      member_KPI_TY.existing_repurchase_member_sales_share - member_KPI_LY.existing_repurchase_member_sales_share                                              AS existing_repurchase_member_sales_share_vs_LY,
      member_KPI_TY.existing_repurchase_member_atv_buying_the_line,
      CAST(member_KPI_TY.existing_repurchase_member_atv_buying_the_line AS FLOAT)/NULLIF(member_KPI_LY.existing_repurchase_member_atv_buying_the_line,0) - 1   AS existing_repurchase_member_atv_buying_the_line_vs_LY,  
      
      
        --- 首购 vs 复购
      member_KPI_TY.lifetime_initial_member_shopper,
      CAST(member_KPI_TY.lifetime_initial_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.lifetime_initial_member_shopper,0) - 1                        AS lifetime_initial_member_shopper_vs_LY,
      member_KPI_TY.lifetime_initial_member_shopper_share,
      member_KPI_TY.lifetime_initial_member_shopper_share - member_KPI_LY.lifetime_initial_member_shopper_share                                       AS lifetime_initial_member_shopper_share_vs_LY,  
      
      member_KPI_TY.lifetime_repurchase_member_shopper                                                                                                AS lifetime_repurchase_member_shopper,
      CAST(member_KPI_TY.lifetime_repurchase_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.lifetime_repurchase_member_shopper,0) - 1                  AS lifetime_repurchase_member_shopper_vs_LY,
      member_KPI_TY.lifetime_repurchase_member_shopper_share,
      member_KPI_TY.lifetime_repurchase_member_shopper_share - member_KPI_LY.lifetime_repurchase_member_shopper_share                                 AS lifetime_repurchase_member_shopper_share_vs_LY
        
FROM sales
LEFT JOIN member_KPI_TY
      ON sales.product_rrp_price_range = member_KPI_TY.product_rrp_price_range
     AND sales.cn_line = member_KPI_TY.cn_line
LEFT JOIN member_KPI_LY
       ON sales.product_rrp_price_range = member_KPI_LY.product_rrp_price_range
     AND sales.cn_line = member_KPI_LY.cn_line;

       
grant select on tutorial.mz_portfolio_lcs_by_cnline_and_price_range to lego_bi_group;
