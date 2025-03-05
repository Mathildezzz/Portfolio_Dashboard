delete from tutorial.mz_member_1_to_2_repurchase_days_distribution;  -- for the subsequent update
insert into tutorial.mz_member_1_to_2_repurchase_days_distribution


WITH omni_trans_fact as
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
    WHERE 1 = 1
      and source_channel in ('LCS','TMALL', 'DOUYIN', 'DOUYIN_B2B')
      and date(tr.order_paid_date) < current_date
      and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
    ),

  
  cte AS (
    SELECT omni_channel_member_id,
        
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)                                                                             AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0) AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) ,0)                                          AS member_frequency
       from omni_trans_fact trans
      WhERE is_member_order = TRUE 
        AnD if_eff_order_tag = true
       GROUP BY 1
         ),
       
initial_and_second_purchase AS (
      SELECT DISTINCT
              omni_channel_member_id,
              order_paid_date,
              LAG(order_paid_date) OVER (PARTITION BY omni_channel_member_id ORDER BY order_paid_date) AS prev_order_paid_date
          FROM (SELECT DISTINCT 
                       order_paid_date,
                       omni_channel_member_id
                FROM omni_trans_fact 
              WHERE omni_channel_member_id IN (
                                               SELECT omni_channel_member_id
                                                 FROM cte
                                                 WHERE member_frequency = 2
                                              )
                ORDER BY 1,2
               )
      ORDER BY 1,2
)



SELECT  CASE WHEN order_paid_date - prev_order_paid_date < 50 THEN '<50'
             WHEN order_paid_date - prev_order_paid_date >= 50 AND order_paid_date - prev_order_paid_date < 100 THEN '50-100'
             WHEN order_paid_date - prev_order_paid_date >= 100 AND order_paid_date - prev_order_paid_date < 150 THEN '100-150'
             WHEN order_paid_date - prev_order_paid_date >= 150 AND order_paid_date - prev_order_paid_date < 200 THEN '150-200'
             WHEN order_paid_date - prev_order_paid_date >= 200 AND order_paid_date - prev_order_paid_date < 250 THEN '200-250'
             WHEN order_paid_date - prev_order_paid_date >= 250 AND order_paid_date - prev_order_paid_date < 300 THEN '250-300'
             WHEN order_paid_date - prev_order_paid_date >= 300 AND order_paid_date - prev_order_paid_date < 350 THEN '300-350'
             WHEN order_paid_date - prev_order_paid_date >= 350 AND order_paid_date - prev_order_paid_date < 400 THEN '350-400'
             WHEN order_paid_date - prev_order_paid_date >= 400 AND order_paid_date - prev_order_paid_date < 450 THEN '400-450'
             WHEN order_paid_date - prev_order_paid_date >= 450 AND order_paid_date - prev_order_paid_date < 500 THEN '450-500'
             WHEN order_paid_date - prev_order_paid_date >= 500 AND order_paid_date - prev_order_paid_date < 550 THEN '500-550'
             WHEN order_paid_date - prev_order_paid_date >= 550 AND order_paid_date - prev_order_paid_date < 600 THEN '550-600'
             WHEN order_paid_date - prev_order_paid_date >= 600 AND order_paid_date - prev_order_paid_date < 650 THEN '600-650'
             WHEN order_paid_date - prev_order_paid_date >= 650 AND order_paid_date - prev_order_paid_date < 700 THEN '650-700'
             WHEN order_paid_date - prev_order_paid_date >= 700 AND order_paid_date - prev_order_paid_date < 750 THEN '700-750'
             WHEN order_paid_date - prev_order_paid_date >= 750 AND order_paid_date - prev_order_paid_date < 800 THEN '750-800'
             WHEN order_paid_date - prev_order_paid_date >= 800 AND order_paid_date - prev_order_paid_date < 850 THEN '800-850'
             WHEN order_paid_date - prev_order_paid_date >= 850 AND order_paid_date - prev_order_paid_date < 900 THEN '850-900'
             WHEN order_paid_date - prev_order_paid_date >= 900 AND order_paid_date - prev_order_paid_date < 950 THEN '900-950'
             WHEN order_paid_date - prev_order_paid_date >= 950 AND order_paid_date - prev_order_paid_date < 1000 THEN '950-1000'
             WHEN order_paid_date - prev_order_paid_date >= 1000 THEN '1000+'
        END AS repurchase_days,
        COUNT(DISTINCT omni_channel_member_id) AS member_shopper_count
  FROM initial_and_second_purchase
WHERE prev_order_paid_date IS NOT NULL
GROUP BY 1;