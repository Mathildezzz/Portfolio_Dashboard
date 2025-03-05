delete from tutorial.mz_portfolio_lcs_by_if_novelty;  -- for the subsequent update
insert into tutorial.mz_portfolio_lcs_by_if_novelty

WITH sales_plan_cte AS (
SELECT plan.lego_sku_id,
       product.lego_sku_name_cn,
       lego_year                 AS plan_year,
       lego_month                AS plan_month,
       product.cn_line           AS cn_line,   
       product.cn_lcs_launch_date,
       product.rsp,
       SUM(sku_plan_qty) AS plan_qty
    FROM mg.sku_plan_monthly plan
LEFT JOIN edw.d_dl_product_info_latest product
       ON plan.lego_sku_id = product.lego_sku_id
WHERE lego_year = extract('year' from current_date)
  AND plan_month <= extract('month' FROM current_date)
AND review_channel_dp LIKE '%LCS%' 
AND dp_version = 'DP02'
GROUP BY 1,2,3,4,5,6,7
),

sales_plan_table AS (
SELECT CASE WHEN extract('year' FROM cn_lcs_launch_date) = extract('year' from current_date) THEN 'novelty' ELSE 'existing' END AS if_novelty,
       SUM(rsp*plan_qty) AS sales_plan
 FROM sales_plan_cte
 GROUP BY 1
 UNION ALL
 SELECT 'TTL'            AS if_novelty,
       SUM(rsp*plan_qty) AS sales_plan
 FROM sales_plan_cte
 GROUP BY 1
 ),


ttl_sales_TY AS (
SELECT CASE WHEN extract('year' FROM bu_cn_launch_date) = extract('year' from current_date) THEN 'novelty' ELSE 'existing' END       AS if_novelty,
       COUNT(DISTINCT parent_order_id)                                                                                               AS transactions,
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
  AND extract('year' FROM DATE(date_id)) = extract('year' from current_date)
  GROUP BY 1
 UNION ALL
 SELECT 'TTL' AS if_novelty,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
  AND extract('year' FROM DATE(date_id)) = extract('year' from current_date)
  GROUP BY 1
  ),
  
ttl_sales_LY AS (
SELECT CASE WHEN extract('year' FROM bu_cn_launch_date) = extract('year' FROM current_date) - 1 THEN 'novelty' ELSE 'existing' END AS if_novelty,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
   AND extract('year' FROM DATE(date_id)) = extract('year' FROM current_date) - 1 -- 年份去年
   AND DATE(date_id) < (current_date - interval '1 year')::date -- 小于去年同一天
   GROUP BY 1
   UNION ALL
SELECT 'TTL' AS if_novelty,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
   AND extract('year' FROM DATE(date_id)) = extract('year' FROM current_date) - 1 -- 年份去年
   AND DATE(date_id) < (current_date - interval '1 year')::date -- 小于去年同一天
   GROUP BY 1
  ),
  
  
  
sales AS (
SELECT ttl_sales_TY.if_novelty,
       ttl_sales_TY.sales_rrp,
       sales_plan_table.sales_plan,
       ttl_sales_TY.sales_rrp/NULLIF(sales_plan_table.sales_plan,0)                                                                           AS sales_progress,
       ttl_sales_TY.transactions,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/ttl_sales_TY.transactions                                                                        AS atv,
       
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/(SELECT SUM(sales_rrp) FROM ttl_sales_TY WHERE if_novelty <> 'TTL')                              AS sales_rrp_share,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/ttl_sales_LY.sales_rrp - 1                                                                       AS sales_vs_LY,
       CAST(ttl_sales_TY.transactions AS FLOAT)/ttl_sales_LY.transactions - 1                                                                 AS transactions_vs_LY,
       (CAST(ttl_sales_TY.sales_rrp AS FLOAT)/ttl_sales_TY.transactions)/(CAST(ttl_sales_LY.sales_rrp AS FLOAT)/ttl_sales_LY.transactions) -1 AS atv_vs_LY
FROM ttl_sales_TY
LEFT JOIN ttl_sales_LY
       ON ttl_sales_TY.if_novelty = ttl_sales_LY.if_novelty
LEFT JOIN sales_plan_table
      ON ttl_sales_TY.if_novelty = sales_plan_table.if_novelty
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
       bu_cn_launch_date,
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
LEFT JOIN edw.d_dl_product_info_latest product
       ON tr.lego_sku_id = product.lego_sku_id
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
      AND extract('year' FROM DATE(join_time)) = extract('year' from current_date)
      AND eff_reg_channel LIKE '%LCS%'
  ),
  
 new_member_ly AS (
  SELECT DISTINCT member_detail_id
     FROM edw.d_member_detail
     WHERE  1= 1
      AND extract('year' FROM DATE(join_time)) = extract('year' FROM current_date) - 1 -- 年份去年
      AND DATE(join_time) < (current_date - interval '1 year')::date -- 小于去年同一天
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
  SELECT CASE WHEN extract('year' FROM bu_cn_launch_date) = extract('year' FROM current_date) THEN 'novelty' ELSE 'existing' END AS if_novelty,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)             AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                     AS member_frequency,
          
          ----------- new ------------
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_mbr_sales_share,
          
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                                       AS new_member_shopper_share,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) AS new_member_shopper,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)     AS existing_member_shopper,
          
                --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_purchase_penetration,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS repurchase_member_shopper

    from omni_trans_fact trans
 LEFT JOIN new_member_ty
        ON trans.member_detail_id::integer = new_member_ty.member_detail_id::integer
 LEFT JOIN (SELECT parent_order_id FROM purchase_order_rk WHERE rk = 1) lifetime_initial_orders
            ON trans.parent_order_id = lifetime_initial_orders.parent_order_id
 where 1 = 1
    and extract('year' FROM DATE(order_paid_date)) = extract('year' from current_date)
GROUP BY 1
UNION ALL

 SELECT 'TTL' AS if_novelty,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0) AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                         AS member_frequency,
         
          ----------- new ------------
          CAST((sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) as new_mbr_sales_share,
          
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                                       AS new_member_shopper_share,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) AS new_member_shopper,
          CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)     AS existing_member_shopper,
        
                  --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_purchase_penetration,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS repurchase_member_shopper

   from omni_trans_fact trans
 LEFT JOIN new_member_ty
        ON trans.member_detail_id::integer = new_member_ty.member_detail_id::integer
 LEFT JOIN (SELECT parent_order_id FROM purchase_order_rk WHERE rk = 1) lifetime_initial_orders
            ON trans.parent_order_id = lifetime_initial_orders.parent_order_id
 where 1 = 1
    and extract('year' FROM DATE(order_paid_date)) = extract('year' from current_date)  
    ),
    
member_KPI_LY AS (
  SELECT CASE WHEN extract('year' FROM bu_cn_launch_date) = extract('year' FROM current_date) - 1 THEN 'novelty' ELSE 'existing' END AS if_novelty,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)             AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                     AS member_frequency,
          
          ----------- new ------------
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) AS new_mbr_sales_share,
          
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                                                                                                        AS new_member_shopper_share,
          
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) AS new_member_shopper,
          CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)     AS existing_member_shopper,
        
        --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_purchase_penetration,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS repurchase_member_shopper

       from omni_trans_fact trans
 LEFT JOIN new_member_ly
        ON trans.member_detail_id::integer = new_member_ly.member_detail_id::integer
 LEFT JOIN (SELECT parent_order_id FROM purchase_order_rk WHERE rk = 1) lifetime_initial_orders
            ON trans.parent_order_id = lifetime_initial_orders.parent_order_id
 where 1 = 1
   AND extract('year' FROM DATE(order_paid_date)) = extract('year' FROM current_date) - 1 -- 年份去年
   AND DATE(order_paid_date) < (current_date - interval '1 year')::date -- 小于去年同一天
GROUP BY 1
UNION ALL

 SELECT 'TTL' AS if_novelty,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)             AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                     AS member_frequency,
          
          ----------- new ------------
          CAST((sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0) AS new_mbr_sales_share,
          
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)/ NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                                                                                                        AS new_member_shopper_share,
         
          CAST((count(distinct case when new_member_ly.member_detail_id IS NOT NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) AS new_member_shopper,
          CAST((count(distinct case when new_member_ly.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)     AS existing_member_shopper,
        
         --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NOT NULL THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_purchase_penetration,
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND lifetime_initial_orders.parent_order_id IS NULL THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS repurchase_member_shopper

  from omni_trans_fact trans
 LEFT JOIN new_member_ly
        ON trans.member_detail_id::integer = new_member_ly.member_detail_id::integer
 LEFT JOIN (SELECT parent_order_id FROM purchase_order_rk WHERE rk = 1) lifetime_initial_orders
            ON trans.parent_order_id = lifetime_initial_orders.parent_order_id
 where 1 = 1
   AND extract('year' FROM DATE(order_paid_date)) = extract('year' FROM current_date) - 1 -- 年份去年
   AND DATE(order_paid_date) < (current_date - interval '1 year')::date -- 小于去年同一天
    )
    
SELECT sales.*,
      member_KPI_TY.member_shopper,
      member_KPI_TY.member_sales,
      member_KPI_TY.member_atv,
      member_KPI_TY.member_frequency,
      
      member_KPI_TY.new_member_shopper,
      
      CAST(member_KPI_TY.new_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.new_member_shopper,0) - 1           AS new_member_shopper_vs_LY,
      member_KPI_TY.existing_member_shopper,
      CAST(member_KPI_TY.existing_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.existing_member_shopper,0) - 1 AS existing_member_shopper_vs_LY,
      
      
      member_KPI_TY.new_mbr_sales_share,
      member_KPI_TY.new_mbr_sales_share -  member_KPI_LY.new_mbr_sales_share AS new_mbr_sales_share_vs_LY,
      member_KPI_TY.new_member_shopper_share,
      member_KPI_TY.new_member_shopper_share -  member_KPI_LY.new_member_shopper_share AS new_mbr_shopper_share_vs_LY,
      
      --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
      member_KPI_TY.lifetime_initial_member_shopper,
      member_KPI_TY.repurchase_member_shopper,
      member_KPI_TY.lifetime_initial_purchase_penetration,
        
      CAST(member_KPI_TY.lifetime_initial_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.lifetime_initial_member_shopper,0) - 1                   AS lifetime_initial_member_shopper_vs_LY,
      CAST(member_KPI_TY.repurchase_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.repurchase_member_shopper,0) - 1                               AS repurchase_member_shopper_vs_LY,
      member_KPI_TY.lifetime_initial_purchase_penetration - member_KPI_LY.lifetime_initial_purchase_penetration                                  AS lifetime_initial_purchase_penetration_vs_LY
 
FROM sales
LEFT JOIN member_KPI_TY
      ON sales.if_novelty = member_KPI_TY.if_novelty
LEFT JOIN member_KPI_LY
      ON sales.if_novelty = member_KPI_LY.if_novelty;