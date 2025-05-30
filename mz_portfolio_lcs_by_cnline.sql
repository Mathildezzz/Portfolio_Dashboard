
-- DROP TABLE IF EXiSTS tutorial.mz_portfolio_lcs_by_cnline;
-- CREATE TABLE tutorial.mz_portfolio_lcs_by_cnline AS

delete from tutorial.mz_portfolio_lcs_by_cnline;  -- for the subsequent update
insert into tutorial.mz_portfolio_lcs_by_cnline

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
AND dp_version = 'DP03'
GROUP BY 1,2,3,4,5,6,7
),


sales_plan_table AS (
SELECT cn_line,
       SUM(rsp*plan_qty)                                                             AS sales_plan
 FROM sales_plan_cte
 GROUP BY 1
 UNION ALL
 SELECT 'TTL'            AS cn_line,
       SUM(rsp*plan_qty) AS sales_plan
 FROM sales_plan_cte
 GROUP BY 1
 ),
    
ttl_sales_TY AS (
SELECT sales.cn_line,
       COUNT(DISTINCT parent_order_id) AS transactions,
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
LEFT JOIN edw.d_dl_product_info_latest product
      ON sales.lego_sku_id = product.lego_sku_id
WHERE 1 = 1 
  AND date_id >= '2024-12-30'
  GROUP BY 1
 UNION ALL
 SELECT 'TTL' AS cn_line,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
    AND date_id >= '2024-12-30'
    AND date_id < current_date  --- TY YTD
  GROUP BY 1
  ),
  
ttl_sales_LY AS (
SELECT sales.cn_line,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
LEFT JOIN edw.d_dl_product_info_latest product
      ON sales.lego_sku_id = product.lego_sku_id
WHERE 1 = 1 
    AND date_id >= '2024-01-01' --- LY YTD
    AND date_id <= (current_date - interval '1 year' + interval '2 days')::date
   GROUP BY 1
   UNION ALL
SELECT 'TTL' AS cn_line,
       COUNT(DISTINCT parent_order_id) AS transactions, 
       sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)  AS sales_rrp
FROM dm_view.offline_lcs_cs__by_sku_fnl sales
WHERE 1 = 1 
    AND date_id >= '2024-01-01' --- LY YTD
    AND date_id <= (current_date - interval '1 year' + interval '2 days')::date
   GROUP BY 1
  ),
  
  
sales AS (
SELECT ttl_sales_TY.cn_line,
       ttl_sales_TY.sales_rrp,
       sales_plan_table.sales_plan,
       ttl_sales_TY.sales_rrp/NULLIF(sales_plan_table.sales_plan,0)                              AS sales_progress,
       ttl_sales_TY.transactions,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_TY.transactions, 0)                AS atv,
       
        CAST(ttl_sales_TY.sales_rrp AS FLOAT)/(SELECT SUM(sales_rrp) FROM ttl_sales_TY WHERE cn_line <> 'TTL')  AS sales_rrp_share,
       CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_LY.sales_rrp,0) - 1               AS sales_vs_LY,
       CAST(ttl_sales_TY.transactions AS FLOAT)/NULLIF(ttl_sales_LY.transactions,0)- 1          AS transactions_vs_LY,
       NULLIF((CAST(ttl_sales_TY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_TY.transactions,0))/NULLIF((CAST(ttl_sales_LY.sales_rrp AS FLOAT)/NULLIF(ttl_sales_LY.transactions,0)),0) - 1,0) AS atv_vs_LY
FROM ttl_sales_TY
LEFT JOIN ttl_sales_LY
       ON ttl_sales_TY.cn_line = ttl_sales_LY.cn_line
LEFT JOIN sales_plan_table
       ON ttl_sales_TY.cn_line = sales_plan_table.cn_line
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
  
 -------------- 会员同单连带 -----------------------------------
  
 bundle_different_sku_orders AS (
   SELECT DISTINCT parent_order_id
     FROM omni_trans_fact
    WHERE if_eff_order_tag = TRUE
      AND is_member_order = TRUE
    GROUP BY 1
   HAVING COUNT(DISTINCT lego_sku_id) >=2
 ),
 
  bundle_different_cnline_orders AS (
   SELECT DISTINCT parent_order_id
     FROM omni_trans_fact
    WHERE if_eff_order_tag = TRUE
      AND is_member_order = TRUE
    GROUP BY 1
   HAVING COUNT(DISTINCT cn_line) >=2
 ),
 
 ------------------------------ YTD top 3 bundled lines 
 bundle_transactions_log AS (
 SELECT DISTINCT parent_order_id, cn_line
     FROM omni_trans_fact
    WHERE if_eff_order_tag = TRUE
      AND is_member_order = TRUE
      AND order_paid_date >= '2024-12-30'
 ),
 
 
 Bundles AS (
    SELECT
        t1.cn_line AS main_line,
        t2.cn_line AS bundled_line,
        COUNT(*)   AS bundle_count
    FROM bundle_transactions_log t1
    JOIN
        bundle_transactions_log t2 
      ON t1.parent_order_id = t2.parent_order_id
    WHERE
        t1.cn_line <> t2.cn_line
    GROUP BY
        t1.cn_line, t2.cn_line
),

TotalBundles AS (
  SELECT cn_line AS main_line,
         COUNT(DISTINCT parent_order_id) AS ttl_bundled_order_cnt
  FROM omni_trans_fact
WHERE if_eff_order_tag = TRUE
 AND is_member_order = TRUE
 AND parent_order_id IN (
                              SELECT DISTINCT parent_order_id
                                 FROM omni_trans_fact
                                WHERE if_eff_order_tag = TRUE
                                  AND is_member_order = TRUE
                                  AND order_paid_date >= '2024-12-30'
                                GROUP BY 1
                              HAVING COUNT(DISTINCT cn_line) >=2
                             ) 
 GROUP BY 1
),

RankedBundles AS (
    SELECT
        b.main_line,
        b.bundled_line,
        b.bundle_count,
        tb.ttl_bundled_order_cnt,
        CAST(b.bundle_count AS FLOAT) / tb.ttl_bundled_order_cnt AS ratio,
        ROW_NUMBER() OVER (PARTITION BY b.main_line ORDER BY b.bundle_count DESC) AS rank
    FROM
        Bundles b
    JOIN
        TotalBundles tb ON b.main_line = tb.main_line
),

bundle_different_cnline_orders_top_3 AS (
    SELECT
        main_line,
        MAX(ttl_bundled_order_cnt)                    AS ttl_bundled_cnline_orders,
        MAX(CASE WHEN rank = 1 THEN bundled_line END) AS top1_bundled_line,
        MAX(CASE WHEN rank = 2 THEN bundled_line END) AS top2_bundled_line,
        MAX(CASE WHEN rank = 3 THEN bundled_line END) AS top3_bundled_line,
        MAX(CASE WHEN rank = 1 THEN ratio END)        AS top1_bundled_penetration,
        MAX(CASE WHEN rank = 2 THEN ratio END)        AS top2_bundled_penetration,
        MAX(CASE WHEN rank = 3 THEN ratio END)        AS top3_bundled_penetration
    FROM
        RankedBundles
    GROUP BY
        main_line
),
 
 ------------------- lifetime purchase ranking -----------------------------
 purchase_order_rk AS (
 SELECT *, 
       ROW_NUMBER () OVER (PARTITION BY omni_channel_member_id ORDER BY order_paid_time ASC) AS rk
  FROM
     (
          SELECT DISTINCT parent_order_id, MIN(order_paid_time) AS order_paid_time, omni_channel_member_id
         FROM omni_trans_fact
        WHERE if_eff_order_tag = TRUE
          AND is_member_order = TRUE
     GROUP BY 1,3
     )
 ),


----------------------------------------------------------------------------------------------
------------- lifetime initial and repurchase happened in this year

initial_purchase_by_cnline AS (
 SELECT DISTINCT omni_channel_member_id,
        cn_line
FROM omni_trans_fact
WHERE parent_order_id IN (SELECT parent_order_id FROM purchase_order_rk WHERE rk = 1)
   AND if_eff_order_tag = TRUE
   AND is_member_order = TRUE
   AND order_paid_date >= '2024-12-30'  -- TY YTD lifetime initial purchase
),

repurchase_by_cnline AS (
 SELECT DISTINCT omni_trans_fact.omni_channel_member_id,
        omni_trans_fact.cn_line,
        purchase_order_rk.rk
FROM omni_trans_fact
LEFT JOIN purchase_order_rk
       ON omni_trans_fact.parent_order_id = purchase_order_rk.parent_order_id
WHERE omni_trans_fact.parent_order_id IN (SELECT parent_order_id FROM purchase_order_rk WHERE rk >= 2)
  AND if_eff_order_tag = TRUE
  AND is_member_order = TRUE
  AND order_paid_date >= '2024-12-30'   -- TY YTD lifetime repurchase
),


repurchase_summary_by_initial_cnline AS ( 
 SELECT initial.cn_line                                                                                                                     AS initial_purchase_cnline,
        COUNT(DISTINCT initial.omni_channel_member_id)                                                                                      AS initial_purchase_member_shopper,
        COUNT(DISTINCT repurchase_by_cnline.omni_channel_member_id)                                                                         AS initial_member_shopper_then_repurchased_member_shopper,
        CAST(COUNT(DISTINCT repurchase_by_cnline.omni_channel_member_id) AS FLOAT)/NULLIF(COUNT(DISTINCT initial.omni_channel_member_id),0) AS initial_member_shopper_repurchase_rate,
        COUNT(DISTINCT CASE WHEN (repurchase_by_cnline.cn_line LIKE '%LEL%')
                          AND repurchase_by_cnline.omni_channel_member_id IS NOT NULL THEN initial.omni_channel_member_id ELSE NULL END)    AS initial_member_shopper_repurchased_LEL,               
        COUNT(DISTINCT CASE WHEN (repurchase_by_cnline.cn_line = initial.cn_line)
                          AND repurchase_by_cnline.omni_channel_member_id IS NOT NULL THEN initial.omni_channel_member_id ELSE NULL END)    AS initial_member_shopper_repurchased_initial_cnline,
        COUNT(DISTINCT CASE WHEN ((repurchase_by_cnline.cn_line = initial.cn_line) OR (repurchase_by_cnline.cn_line LIKE '%LEL%'))
                      AND repurchase_by_cnline.omni_channel_member_id IS NOT NULL THEN initial.omni_channel_member_id ELSE NULL END)         AS initial_member_shopper_repurchased_LEL_or_initial_cnline      
  FROM initial_purchase_by_cnline initial
  LEFT JOIN repurchase_by_cnline
         ON initial.omni_channel_member_id = repurchase_by_cnline.omni_channel_member_id
  GROUP BY 1
  UNION ALL
  SELECT 'TTL'                                                                                                                              AS initial_purchase_cnline,
        COUNT(DISTINCT initial.omni_channel_member_id)                                                                                      AS initial_purchase_member_shopper,
        COUNT(DISTINCT repurchase_by_cnline.omni_channel_member_id)                                                                         AS initial_member_shopper_then_repurchased_member_shopper,
        CAST(COUNT(DISTINCT repurchase_by_cnline.omni_channel_member_id) AS FLOAT)/NULLIF(COUNT(DISTINCT initial.omni_channel_member_id),0) AS initial_member_shopper_repurchase_rate,
        COUNT(DISTINCT CASE WHEN (repurchase_by_cnline.cn_line LIKE '%LEL%')
                          AND repurchase_by_cnline.omni_channel_member_id IS NOT NULL THEN initial.omni_channel_member_id ELSE NULL END)    AS initial_member_shopper_repurchased_LEL,               
        COUNT(DISTINCT CASE WHEN (repurchase_by_cnline.cn_line = initial.cn_line)
                          AND repurchase_by_cnline.omni_channel_member_id IS NOT NULL THEN initial.omni_channel_member_id ELSE NULL END)    AS initial_member_shopper_repurchased_initial_cnline,
        COUNT(DISTINCT CASE WHEN ((repurchase_by_cnline.cn_line = initial.cn_line) OR (repurchase_by_cnline.cn_line LIKE '%LEL%'))
                      AND repurchase_by_cnline.omni_channel_member_id IS NOT NULL THEN initial.omni_channel_member_id ELSE NULL END)         AS initial_member_shopper_repurchased_LEL_or_initial_cnline 
  FROM initial_purchase_by_cnline initial
  LEFT JOIN repurchase_by_cnline
         ON initial.omni_channel_member_id = repurchase_by_cnline.omni_channel_member_id
         GROUP BY 1
),


repurchase_cnline_by_initial_cnline AS (
SELECT initial_purchase_cnline,
       repurchase_cnline,
       repurchased_member_count,
       ROW_NUMBER () OVER (PARTITION BY initial_purchase_cnline ORDER BY repurchased_member_count DESC) AS rk
 FROM (
          SELECT initial.cn_line                 AS initial_purchase_cnline,
                repurchase_by_cnline.cn_line     AS repurchase_cnline,
                COUNT(DISTINCT repurchase_by_cnline.omni_channel_member_id) AS repurchased_member_count
              FROM initial_purchase_by_cnline initial
        INNER JOIN repurchase_by_cnline               --- 只看repurchase的人
                ON initial.omni_channel_member_id = repurchase_by_cnline.omni_channel_member_id
          GROUP BY 1,2
      )
),

repurchase_top_3_cnline_by_initial_cnline AS (
SELECT 
    initial_purchase_cnline,
    MAX(CASE WHEN rk = 1 THEN repurchase_cnline ELSE NULL END) AS repurchase_cnline_1,
    MAX(CASE WHEN rk = 2 THEN repurchase_cnline ELSE NULL END) AS repurchase_cnline_2,
    MAX(CASE WHEN rk = 3 THEN repurchase_cnline ELSE NULL END) AS repurchase_cnline_3,
    MAX(CASE WHEN rk = 4 THEN repurchase_cnline ELSE NULL END) AS repurchase_cnline_4
FROM repurchase_cnline_by_initial_cnline
GROUP BY 1
),


-------------------------------------------------------------------------------------------------------------
-- 复购发生在今年
previous_purchase_summary_by_repurchase_cnline AS (
 SELECT repurchase.cn_line                                                                                                       AS lifetime_repurchase_cnline,
        COUNT(DISTINCT repurchase.omni_channel_member_id)                                                                        AS lifetime_repurchase_member_shopper,
         COUNT(DISTINCT CASE WHEN (previous_purchases.cn_line = repurchase.cn_line)
                      AND repurchase.omni_channel_member_id IS NOT NULL THEN repurchase.omni_channel_member_id ELSE NULL END)    AS lifetime_repurchase_member_shopper_previously_purchased_the_line
  FROM repurchase_by_cnline repurchase
  LEFT JOIN (SELECT DISTINCT omni_trans_fact.omni_channel_member_id, omni_trans_fact.cn_line, purchase_order_rk.rk
              FROM omni_trans_fact
          LEFT JOIN purchase_order_rk
                 ON omni_trans_fact.parent_order_id = purchase_order_rk.parent_order_id
            WHERE if_eff_order_tag = TRUE
              AND is_member_order = TRUE
          ) previous_purchases
         ON repurchase.omni_channel_member_id = previous_purchases.omni_channel_member_id
        AND repurchase.rk > previous_purchases.rk          ---- 所有以前的purchase
 GROUP BY 1
),
    
                          
                          
previous_purchase_cnline_by_repurchase_cnline AS (
SELECT lifetime_repurchase_cnline,
       previous_purchases_cnline,
       member_count,
       ROW_NUMBER () OVER (PARTITION BY lifetime_repurchase_cnline ORDER BY member_count DESC) AS rk
FROM (
 SELECT repurchase.cn_line                                    AS lifetime_repurchase_cnline,
        previous_purchases.cn_line                            AS previous_purchases_cnline,
        COUNT(DISTINCT repurchase.omni_channel_member_id)     AS member_count
  FROM repurchase_by_cnline repurchase
  LEFT JOIN (SELECT DISTINCT omni_trans_fact.omni_channel_member_id, omni_trans_fact.cn_line, purchase_order_rk.rk
              FROM omni_trans_fact
          LEFT JOIN purchase_order_rk
                 ON omni_trans_fact.parent_order_id = purchase_order_rk.parent_order_id
            WHERE if_eff_order_tag = TRUE
              AND is_member_order = TRUE
          ) previous_purchases
         ON repurchase.omni_channel_member_id = previous_purchases.omni_channel_member_id
        AND repurchase.rk > previous_purchases.rk          ---- 所有以前的purchase
 GROUP BY 1,2
      )
),

previous_purchase_top_3_cnline_by_repurchase_cnline AS (
SELECT 
    lifetime_repurchase_cnline,
    MAX(CASE WHEN rk = 1 THEN previous_purchases_cnline ELSE NULL END) AS previous_purchases_cnline_1,
    MAX(CASE WHEN rk = 2 THEN previous_purchases_cnline ELSE NULL END) AS previous_purchases_cnline_2,
    MAX(CASE WHEN rk = 3 THEN previous_purchases_cnline ELSE NULL END) AS previous_purchases_cnline_3,
    MAX(CASE WHEN rk = 4 THEN previous_purchases_cnline ELSE NULL END) AS previous_purchases_cnline_4
FROM previous_purchase_cnline_by_repurchase_cnline
GROUP BY 1
),



--------------------------------------------------------------------------------------------------------------
-------------------------------  member profile

---------- cdp 送礼人群  (人群定义还需要看看，现在ratio过大)
-- seg as (
--  select gio_id
--         ,row_number() over ( partition by gio_id order by update_time desc) as rk
--   from stg.gio_user_local 
--   where prop_key = 'tag_sfwslrq'
--   qualify rk = 1 
--   and prop_value = '是'
-- ),

-- members as (
--  select gio_id
--         ,prop_value as crm_member_id
--         ,row_number() over ( partition by gio_id order by update_time desc) as rk
--   from stg.gio_user_local where prop_key = 'usr_crm_member_id'
--   qualify rk = 1
-- ),

-- member_profile_gifting AS (
-- select DISTINCT
--       m.gio_id
--       ,m.crm_member_id
-- from members m
-- join seg s
-- on m.gio_id = s.gio_id
-- ),

-----------------      
member_profile AS (
SELECT DISTINCT 
       mbr.member_detail_id,
       mbr.tier_code,
       CASE WHEN mbr.gender = 1 THEN 'male' 
            WHEN mbr.gender = 2 THEN 'female'
            WHEN mbr.gender = 0 THEN 'gender_unknown' END                                                                 AS gender,
            
        CASE WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday)))  < 18 THEN '<18'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >= 18 AND (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) <= 25 THEN '18-25'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >= 26 AND (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) <= 30 THEN '26-30'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >= 31 AND (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) <= 35 THEN '31-35'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >= 36 AND (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) <= 40 THEN '36-40'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >= 41 AND (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) <= 45 THEN '41-45'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >= 46 AND (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) <= 50 THEN '46-50'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >=51 AND (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) <= 55 THEN '51-55'
            WHEN (2025 - EXTRACT('year' FROM DATE(mbr.birthday))) >= 56 THEN '56+'
        ELSE 'age_unknown'  END                                                                                                AS age_group,
       CASE WHEN beneficiary_birthday IS NULL THEN 0 ELSE 1 END                                                                AS has_birthday,
       CASE WHEN (kids_birthday_table.member_detail_id IS NOT NULL) OR (mbr.has_kid = 1) THEN 1 ELSE 0 END                     AS has_kid,
       
            kids_birthday_table.has_kid_0_to_5,
            kids_birthday_table.has_kid_6_to_8, 
            kids_birthday_table.has_kid_9_to_12, 
            kids_birthday_table.has_kid_13_to_17, 
            kids_birthday_table.has_kid_18_plus
  FROM edw.d_member_detail mbr
  LEFT JOIN (SELECT member_detail_id,
                       MAX(CASE WHEN (2025 - kids_birthday.birthday_year) <= 5 THEN 1 ELSE 0 END)                                                AS has_kid_0_to_5,
                       MAX(CASE WHEN (2025 - kids_birthday.birthday_year) >= 6 AND (2025 - kids_birthday.birthday_year)  <= 8 THEN 1 ELSE 0 END)    AS has_kid_6_to_8, 
                       MAX(CASE WHEN (2025 - kids_birthday.birthday_year) >= 9 AND (2025 - kids_birthday.birthday_year)  <= 12 THEN 1 ELSE 0 END)  AS has_kid_9_to_12, 
                       MAX(CASE WHEN (2025 - kids_birthday.birthday_year) >= 13 AND (2025 - kids_birthday.birthday_year)  <= 17 THEN 1 ELSE 0 END)  AS has_kid_13_to_17, 
                       MAX(CASE WHEN (2025 - kids_birthday.birthday_year) >= 18 THEN 1 ELSE 0 END)                                                 AS has_kid_18_plus
               FROM edw.d_dl_crm_birthday_history kids_birthday
               WHERE person_type = 2
               GROUP BY 1
             ) kids_birthday_table
         ON mbr.member_detail_id::integer = kids_birthday_table.member_detail_id::integer
),
        

---------------------------------------------------------------------------------------------------------------

 member_KPI_TY AS (
  SELECT trans.cn_line,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)             AS member_atv_buying_the_line,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                     AS member_frequency_buying_the_line,
          
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
        
        -----------------------------------------------
        ------ 同单连带
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND bundle_different_sku_orders.parent_order_id IS NOT NULL THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.parent_order_id ELSE NULL END),0)  AS bundle_differnt_sku_order_rate,
         CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND bundle_different_cnline_orders.parent_order_id IS NOT NULL THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.parent_order_id ELSE NULL END),0)  AS bundle_differnt_cnline_order_rate,
        
        
        ----------------
        --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_member_shopper_share,
        
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_repurchase_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share,
      
        -- --- 第二次购买 （为了分析purchase product path
        -- COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 2 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_second_member_shopper,
        -- CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_second_purchase_penetration,


        -- --- 第三次购买
        -- COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 3 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_third_member_shopper,
        -- CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 3 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_third_purchase_penetration,
        
        -- --- 第四次购买
        -- COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 4 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_fourth_member_shopper,
        -- CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 4 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_fourth_purchase_penetration,

        --  --- 五次及以上
        -- COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 5 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_5th_and_later_member_shopper,
        -- CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 5 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_5th_and_later_purchase_penetration,
  
        --  首单买完此线，后续的repurchase rate
        
        
        --- 作为首单，后续的repurchase1什么线
        
        --------------------------------------
        -----profile
        
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid = 1 then trans.member_detail_id else null end)),0)           AS member_shopper_profile_has_kid,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_0_to_5 = 1 then trans.member_detail_id else null end)),0)    AS member_shopper_profile_has_kid_0_to_5,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_6_to_8 = 1 then trans.member_detail_id else null end)),0)    AS member_shopper_profile_has_kid_6_to_8,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_9_to_12 = 1 then trans.member_detail_id else null end)),0)   AS member_shopper_profile_has_kid_9_to_12,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_13_to_17 = 1 then trans.member_detail_id else null end)),0)  AS member_shopper_profile_has_kid_13_to_17,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_18_plus = 1 then trans.member_detail_id else null end)),0)   AS member_shopper_profile_has_kid_18_plus,
       
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '1_mature' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_city_maturity_type_1_mature,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '2_core' then trans.member_detail_id else null end)),0)             AS member_shopper_profile_city_maturity_type_2_core,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '3_uprising' then trans.member_detail_id else null end)),0)         AS member_shopper_profile_city_maturity_type_3_uprising,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '4_unspecified' then trans.member_detail_id else null end)),0)      AS member_shopper_profile_city_maturity_type_4_unspecified,
         
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 1' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_1,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 2' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_2,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 3' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_3,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 4' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_4,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 5' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_5,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 6' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_6,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'unspecified' then trans.member_detail_id else null end)),0)     AS member_shopper_profile_city_tier_unspecified,
         
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '< 18' then trans.member_detail_id else null end)),0)            AS member_shopper_profile_age_less_than_18,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '18-25' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_18_to_25,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '26-30' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_26_to_30,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '31-35' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_31_to_35,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '36-40' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_36_to_40,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '41-45' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_41_to_45,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '46-50' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_46_to_50,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '51-55' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_51_to_55,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '56+' then trans.member_detail_id else null end)),0)             AS member_shopper_profile_age_56_plus,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = 'age_unknown' then trans.member_detail_id else null end)),0)     AS member_shopper_profile_age_unknown,

         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gender = 'male' then trans.member_detail_id else null end)),0)               AS member_shopper_profile_gender_male,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gender = 'female' then trans.member_detail_id else null end)),0)             AS member_shopper_profile_gender_female,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gender = 'gender_unknown' then trans.member_detail_id else null end)),0)     AS member_shopper_profile_gender_unknown
         
        --  NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gifting = 1 then trans.member_detail_id else null end)),0)                   AS member_shopper_profile_gifting
    
    from omni_trans_fact trans
     LEFT JOIN new_member_ty
            ON trans.member_detail_id::integer = new_member_ty.member_detail_id::integer
     LEFT JOIN bundle_different_sku_orders
            ON trans.parent_order_id = bundle_different_sku_orders.parent_order_id
     LEFT JOIN bundle_different_cnline_orders
            ON trans.parent_order_id = bundle_different_cnline_orders.parent_order_id  
     LEFT JOIN purchase_order_rk
            ON trans.parent_order_id = purchase_order_rk.parent_order_id
     LEFT JOIN member_profile
            ON trans.member_detail_id::integer = member_profile.member_detail_id::integer
    where 1 = 1
    and DATE(order_paid_date) >= '2024-12-30'
    GROUP BY 1
    UNION ALL
  SELECT 'TTL' AS cn_line,
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0) AS member_atv_buying_the_line,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                         AS member_frequency_buying_the_line,
         
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
          CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk = 1) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk = 1) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk = 1) then trans.parent_order_id else null end),0)                     AS existing_0_1_member_atv_buying_the_line,
      
         ----------- existing repurchase
         
         CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT)                                                                                                                                                  AS existing_repurchase_member_shopper,
         CAST((count(distinct case when new_member_ty.member_detail_id IS NULL AND ( purchase_order_rk.rk >= 2) AND is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)             AS existing_repurchase_member_shopper_share,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)       AS existing_repurchase_member_sales,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF((sum(case when is_member_order = TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order = TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)),0)                                  AS existing_repurchase_member_sales_share,
         CAST((sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty > 0 AND (purchase_order_rk.rk >= 2) then order_rrp_amt else 0 end) - sum(case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND sales_qty < 0 AND (purchase_order_rk.rk >= 2) then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when new_member_ty.member_detail_id IS NULL AND is_member_order IS TRUE AND if_eff_order_tag = true  AND (purchase_order_rk.rk >=2 ) then trans.parent_order_id else null end),0)                AS existing_repurchase_member_atv_buying_the_line,
      
         
          
        -----------------------------------------------
        ------ 同单连带
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND bundle_different_sku_orders.parent_order_id IS NOT NULL THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.parent_order_id ELSE NULL END),0)  AS bundle_differnt_sku_order_rate,
         CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND bundle_different_cnline_orders.parent_order_id IS NOT NULL THEN trans.parent_order_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.parent_order_id ELSE NULL END),0)  AS bundle_differnt_cnline_order_rate,
        
        
        ----------------
        --- 首购 （lifetime首购） vs 复购
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_initial_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk = 1 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT)/ NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)  AS lifetime_initial_member_shopper_share,
        
        COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END)                                                                                                                                                          AS lifetime_repurchase_member_shopper,
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share,
      
        --- 作为首单，后续的repurchase1什么线
        
        -----------------------------------------------------
        ---- profile
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid = 1 then trans.member_detail_id else null end)),0)         AS member_shopper_profile_has_kid,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_0_to_5 = 1 then trans.member_detail_id else null end)),0)  AS member_shopper_profile_has_kid_0_to_5,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_6_to_8 = 1 then trans.member_detail_id else null end)),0)  AS member_shopper_profile_has_kid_6_to_8,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_9_to_12 = 1 then trans.member_detail_id else null end)),0)  AS member_shopper_profile_has_kid_9_to_12,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_13_to_17 = 1 then trans.member_detail_id else null end)),0)  AS member_shopper_profile_has_kid_13_to_17,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.has_kid_18_plus = 1 then trans.member_detail_id else null end)),0)  AS member_shopper_profile_has_kid_18_plus,

         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '1_mature' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_city_maturity_type_1_mature,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '2_core' then trans.member_detail_id else null end)),0)             AS member_shopper_profile_city_maturity_type_2_core,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '3_uprising' then trans.member_detail_id else null end)),0)         AS member_shopper_profile_city_maturity_type_3_uprising,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_maturity_type = '4_unspecified' then trans.member_detail_id else null end)),0)      AS member_shopper_profile_city_maturity_type_4_unspecified,
         
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 1' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_1,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 2' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_2,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 3' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_3,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 4' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_4,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 5' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_5,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'Tier 6' then trans.member_detail_id else null end)),0)          AS member_shopper_profile_city_tier_6,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND city_tier = 'unspecified' then trans.member_detail_id else null end)),0)     AS member_shopper_profile_city_tier_unspecified,

         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '< 18' then trans.member_detail_id else null end)),0)            AS member_shopper_profile_age_less_than_18,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '18-25' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_18_to_25,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '26-30' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_26_to_30,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '31-35' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_31_to_35,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '36-40' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_36_to_40,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '41-45' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_41_to_45,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '46-50' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_46_to_50,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '51-55' then trans.member_detail_id else null end)),0)           AS member_shopper_profile_age_51_to_55,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = '56+' then trans.member_detail_id else null end)),0)             AS member_shopper_profile_age_56_plus,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.age_group = 'age_unknown' then trans.member_detail_id else null end)),0)             AS member_shopper_profile_age_unknown,

         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gender = 'male' then trans.member_detail_id else null end)),0)               AS member_shopper_profile_gender_male,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gender = 'female' then trans.member_detail_id else null end)),0)             AS member_shopper_profile_gender_female,
         NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gender = 'gender_unknown' then trans.member_detail_id else null end)),0)     AS member_shopper_profile_gender_unknown
   
        --  NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND member_profile.gifting = 1 then trans.member_detail_id else null end)),0)                   AS member_shopper_profile_gifting
    from omni_trans_fact trans
     LEFT JOIN new_member_ty
            ON trans.member_detail_id::integer = new_member_ty.member_detail_id::integer
     LEFT JOIN bundle_different_sku_orders
            ON trans.parent_order_id = bundle_different_sku_orders.parent_order_id
     LEFT JOIN bundle_different_cnline_orders
            ON trans.parent_order_id = bundle_different_cnline_orders.parent_order_id  
     LEFT JOIN purchase_order_rk
            ON trans.parent_order_id = purchase_order_rk.parent_order_id
     LEFT JOIN member_profile
            ON trans.member_detail_id::integer = member_profile.member_detail_id::integer
    where 1 = 1
    and DATE(order_paid_date) >= '2024-12-30'
    ),
    
    
member_KPI_LY AS (
  SELECT cn_line,
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
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share
      
    from omni_trans_fact trans
     LEFT JOIN new_member_ly
            ON trans.member_detail_id::integer = new_member_ly.member_detail_id::integer
      LEFT JOIN purchase_order_rk
            ON trans.parent_order_id = purchase_order_rk.parent_order_id
    where 1 = 1
      AND DATE(order_paid_date) >= '2024-01-01' --- LY YTD
      AND DATE(order_paid_date) <= (current_date - interval '1 year' + interval '2 days')::date
    GROUP BY 1
    UNION ALL
  SELECT 'TTL' AS cn_line,
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
        CAST(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE AND purchase_order_rk.rk >= 2 THEN trans.omni_channel_member_id ELSE NULL END) AS FLOAT) / NULLIF(COUNT(DISTINCT CASE WHEN is_member_order IS TRUE AND if_eff_order_tag = TRUE THEN trans.omni_channel_member_id ELSE NULL END),0)                AS lifetime_repurchase_member_shopper_share
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
      CAST(member_KPI_TY.member_shopper AS FLOAT)/NULLIF(member_KPI_LY.member_shopper,0) - 1     AS member_shopper_vs_LY,
      member_KPI_TY.member_sales,
      member_KPI_TY.member_sales/NULLIF( member_KPI_LY.member_sales,0) - 1                         AS member_sales_vs_LY,
      member_KPI_TY.member_atv_buying_the_line,
      member_KPI_TY.member_atv_buying_the_line/NULLIF(member_KPI_LY.member_atv,0) - 1             AS member_atv_vs_LY,
      member_KPI_TY.member_frequency_buying_the_line,
      member_KPI_TY.member_frequency_buying_the_line/NULLIF( member_KPI_LY.member_frequency,0) - 1 AS member_frequency_vs_LY,
      
      
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
      
      ------ 同单连带
      member_KPI_TY.bundle_differnt_sku_order_rate,
      member_KPI_TY.bundle_differnt_cnline_order_rate,
      bundle_different_cnline_orders_top_3.*,
     
     
      --- 首购 （lifetime首购） vs 复购： 首购人数，复购人数， 首购penetration
      member_KPI_TY.lifetime_initial_member_shopper,
      CAST(member_KPI_TY.lifetime_initial_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.lifetime_initial_member_shopper,0) - 1                        AS lifetime_initial_member_shopper_vs_LY,
      member_KPI_TY.lifetime_initial_member_shopper_share,
      member_KPI_TY.lifetime_initial_member_shopper_share - member_KPI_LY.lifetime_initial_member_shopper_share                                       AS lifetime_initial_member_shopper_share_vs_LY,  
      
      member_KPI_TY.lifetime_repurchase_member_shopper                                                                                                AS lifetime_repurchase_member_shopper,
      CAST(member_KPI_TY.lifetime_repurchase_member_shopper AS FLOAT)/NULLIF(member_KPI_LY.lifetime_repurchase_member_shopper,0) - 1                  AS lifetime_repurchase_member_shopper_vs_LY,
      member_KPI_TY.lifetime_repurchase_member_shopper_share,
      member_KPI_TY.lifetime_repurchase_member_shopper_share - member_KPI_LY.lifetime_repurchase_member_shopper_share                                 AS lifetime_repurchase_member_shopper_share_vs_LY,
        
      
      -- 首单买完此线，后续的repurchase rate
       
      repurchase_summary_by_initial_cnline.initial_member_shopper_repurchase_rate                                                                      AS lifetime_initial_member_shopper_repurchase_rate,
      
      
      --- 作为首单，后续的repurchase1什么线
      top_3_repurchase_cnline.repurchase_cnline_1 AS repurchase_cnline_top_1,
      top_3_repurchase_cnline.repurchase_cnline_2 AS repurchase_cnline_top_2,
      top_3_repurchase_cnline.repurchase_cnline_3 AS repurchase_cnline_top_3,
      top_3_repurchase_cnline.repurchase_cnline_4 AS repurchase_cnline_top_4,
      
      CAST(repurchase_summary_by_initial_cnline.initial_member_shopper_repurchased_LEL AS FLOAT)/NULLIF(repurchase_summary_by_initial_cnline.initial_member_shopper_then_repurchased_member_shopper,0)                        AS lifetime_initial_member_shopper_repurchased_LEL_member_shopper_share_of_all_repurchased,
      CAST(repurchase_summary_by_initial_cnline.initial_member_shopper_repurchased_initial_cnline AS FLOAT)/NULLIF(repurchase_summary_by_initial_cnline.initial_member_shopper_then_repurchased_member_shopper,0)            AS lifetime_initial_member_shopper_repurchased_initial_cnline_member_shopper_share_of_all_repurchased,
      CAST(repurchase_summary_by_initial_cnline.initial_member_shopper_repurchased_LEL_or_initial_cnline AS FLOAT)/NULLIF(repurchase_summary_by_initial_cnline.initial_member_shopper_then_repurchased_member_shopper,0)     AS lifetime_initial_member_shopper_repurchased_LEL_or_initial_cnline_member_shopper_share_of_all_repurchased,
      
      
    --   ---- 作为复购单，以前买过什么
      previous_purchase_summary_by_repurchase_cnline.lifetime_repurchase_member_shopper                                                                                                                                     AS lifetime_repurchase_member_shopper_v2,
       
      top_3_previous_cnline.previous_purchases_cnline_1,
      top_3_previous_cnline.previous_purchases_cnline_2,
      top_3_previous_cnline.previous_purchases_cnline_3,
      top_3_previous_cnline.previous_purchases_cnline_4,
       
      CAST(previous_purchase_summary_by_repurchase_cnline.lifetime_repurchase_member_shopper_previously_purchased_the_line AS FLOAT)/NULLIF(previous_purchase_summary_by_repurchase_cnline.lifetime_repurchase_member_shopper,0)     AS previous_purchases_repurchase_cnline_share,

       
      ------ member profile
      member_KPI_TY.member_shopper_profile_has_kid,
      member_KPI_TY.member_shopper_profile_has_kid_0_to_5,
      member_KPI_TY.member_shopper_profile_has_kid_6_to_8,
      member_KPI_TY.member_shopper_profile_has_kid_9_to_12,
      member_KPI_TY.member_shopper_profile_has_kid_13_to_17,
      member_KPI_TY.member_shopper_profile_has_kid_18_plus,
      
      member_shopper_profile_city_maturity_type_1_mature,
      member_shopper_profile_city_maturity_type_2_core,
      member_shopper_profile_city_maturity_type_3_uprising,
      member_shopper_profile_city_maturity_type_4_unspecified,
         
      member_shopper_profile_city_tier_1,
      member_shopper_profile_city_tier_2,
      member_shopper_profile_city_tier_3,
      member_shopper_profile_city_tier_4,
      member_shopper_profile_city_tier_5,
      member_shopper_profile_city_tier_6,
      member_shopper_profile_city_tier_unspecified,
      
     member_shopper_profile_age_less_than_18,
     member_shopper_profile_age_18_to_25,
     member_shopper_profile_age_26_to_30,
     member_shopper_profile_age_31_to_35,
     member_shopper_profile_age_36_to_40,
     member_shopper_profile_age_41_to_45,
     member_shopper_profile_age_46_to_50,
     member_shopper_profile_age_51_to_55,
     member_shopper_profile_age_56_plus,
     member_shopper_profile_age_unknown,
     
     member_shopper_profile_gender_male,
     member_shopper_profile_gender_female,
     member_shopper_profile_gender_unknown

FROM sales
LEFT JOIN member_KPI_TY
      ON sales.cn_line = member_KPI_TY.cn_line
 LEFT JOIN bundle_different_cnline_orders_top_3
        ON sales.cn_line = bundle_different_cnline_orders_top_3.main_line
LEFT JOIN member_KPI_LY
      ON sales.cn_line = member_KPI_LY.cn_line
LEFT JOIN repurchase_summary_by_initial_cnline
      ON sales.cn_line = repurchase_summary_by_initial_cnline.initial_purchase_cnline
LEFT JOIN repurchase_top_3_cnline_by_initial_cnline              top_3_repurchase_cnline
      ON sales.cn_line = top_3_repurchase_cnline.initial_purchase_cnline
LEFT JOIN previous_purchase_summary_by_repurchase_cnline
      ON sales.cn_line = previous_purchase_summary_by_repurchase_cnline.lifetime_repurchase_cnline
LEFT JOIN previous_purchase_top_3_cnline_by_repurchase_cnline    top_3_previous_cnline
      ON sales.cn_line = top_3_previous_cnline.lifetime_repurchase_cnline;

      
      
grant select on tutorial.mz_portfolio_lcs_by_cnline to lego_bi_group;
      
  