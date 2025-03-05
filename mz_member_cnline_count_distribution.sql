DELETE FROM tutorial.mz_member_cnline_count_distribution;
INSERT INTO tutorial.mz_member_cnline_count_distribution

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
          NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)),0)                                                                                   AS member_shopper,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS member_sales,
          CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0) AS member_atv,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.omni_channel_member_id else null end)) ,0)                                                   AS member_frequency,
          CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.cn_line else null end)) AS FLOAT)                                   AS cnline_count
       from omni_trans_fact trans
      WhERE is_member_order = TRUE
          AnD if_eff_order_tag = true
       GROUP BY 1
)

SELECT CASE wHen cnline_count >= 5 THEN '5+' ELSE CAST(cnline_count AS text) end                                        AS cnline_count,
       CASt(COUNT(DISTINCT omni_channel_member_id) as FLOAT) / (SELECT COUNT(DISTiNCT omni_channel_member_id) FROM cte) AS member_share 
     FROM CTE
     GROUP BY 1;