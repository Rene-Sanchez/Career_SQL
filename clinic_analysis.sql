with
clinic_ta as (
    select c.super_clinic_id,
           p.patient_pod_category,
           count(distinct delivery_id) as fills,
           row_number() over (partition by super_clinic_id order by fills desc) as ranking
    from analytics.core.fct_delivered_fills as df
    left join analytics.core.dim_patients as p on p.key = df.patient_key
    left join analytics.core.dim_clinics as c on c.key = df.clinic_key
    group by 1,2
    qualify ranking = 1
)
,clinic_fills as ( --------------------------------by clinic by product
    select p.product_name,
           p.brand_vs_generic,
           lower(c.clinic_name) as clinic_name,
           c.super_clinic_id,
           c.wunderbar_id,
            c.state,
           s.patient_pod_category as clinic_ta,
           sum(IFF(coalesce(p.target_medication_class,p.bizops_target_med_class) ilike 'GLP%',11.99,0)) as product_service_fee,
           count(distinct pt.merged_into_user_id ) as total_patients,
           count(distinct df.delivery_id) as total_med_fills,
           count(distinct 
                  case when date_trunc('month',df.delivered_at) = date_trunc('month',pt.created_at) 
                          then df.delivery_id 
                        else null 
                  end) as np_fills,
           round(avg(
                   (df.revenue_with_emd_fee +
                    coalesce(df.dispensing_revenue_adjustment,0) +
                    coalesce(df.progyny_revenue_adjustment,0) -
                    coalesce(df.dir_high,0)
                    ) - (df.cogs - coalesce(df.emd_rebate_adjustment,0))
                    ),3
              ) as avg_profit,
           sum((df.revenue_with_emd_fee +
                coalesce(df.dispensing_revenue_adjustment,0) +
                coalesce(df.progyny_revenue_adjustment,0) -
                coalesce(df.dir_high,0)
                ) - (df.cogs - coalesce(df.emd_rebate_adjustment,0) )
              ) as total_profit,
           round(avg(
                   (df.revenue_with_emd_fee +
                    coalesce(df.dispensing_revenue_adjustment,0) +
                    coalesce(df.progyny_revenue_adjustment,0) -
                    coalesce(df.dir_high,0)
                    )
                    ),3
              ) as avg_revenue,
           sum((df.revenue_with_emd_fee +
                 coalesce(df.dispensing_revenue_adjustment,0) +
                 coalesce(df.progyny_revenue_adjustment,0) -
                 coalesce(df.dir_high,0)
                 )
                ) as total_revenue,
           count( distinct
                IFF((
                    (df.revenue_with_emd_fee +
                      coalesce(df.dispensing_revenue_adjustment,0) +
                      coalesce(df.progyny_revenue_adjustment,0) -
                      coalesce(df.dir_high,0)
                      ) - (df.cogs - coalesce(df.emd_rebate_adjustment,0) )
                   ) < 0 ,
                  df.delivery_id,null )
               ) as neg_gp_fills,
           neg_gp_fills / total_med_fills as negative_gp_rate,
           np_fills/ total_med_fills as pct_fills_from_NPs
    from analytics.core.fct_delivered_fills as df
    left join analytics.core.dim_products as p on p.key = df.product_key
    left join analytics.core.dim_clinics as c on c.key = df.clinic_key
    left join analytics.core.dim_patients as pt on pt.key = df.patient_key
    left join clinic_ta as s on s.super_clinic_id = c.super_clinic_id
    where df.delivered_at::date >= '2022-07-01'----------------------------------------------date range - H2 2022
    and df.delivered_at::date <= '2022-12-31'
    and canceled_at is null
    and clinic_ta not in ('Fertility','HIV','Specialty','Dermatology', 'Core')
    and not p.is_covid_antiviral
    and not(p.product_name ilike any ('%covid%','%Ensure%','%Praluent%','%Repatha%','%Leqvio%','%Prolia%','%Forteo%','%Dupixent%','%Xolair%' ) )
    group by 1,2,3,4,5,6,7
    order by 3
)
,clinic_money_w_mj_weg as ( --------------------------------by clinic by product
    select lower(c.clinic_name) as clinic_name,
           c.super_clinic_id,
           sum((df.revenue_with_emd_fee +
                coalesce(df.dispensing_revenue_adjustment,0) +
                coalesce(df.progyny_revenue_adjustment,0) -
                coalesce(df.dir_high,0)
                ) - (df.cogs - coalesce(df.emd_rebate_adjustment,0) )
              ) as total_profit,
           sum((df.revenue_with_emd_fee +
                 coalesce(df.dispensing_revenue_adjustment,0) +
                 coalesce(df.progyny_revenue_adjustment,0) -
                 coalesce(df.dir_high,0)
                 )
                ) as total_revenue
    from analytics.core.fct_delivered_fills as df
    left join analytics.core.dim_products as p on p.key = df.product_key
    left join analytics.core.dim_clinics as c on c.key = df.clinic_key
    left join analytics.core.dim_patients as pt on pt.key = df.patient_key
    left join clinic_ta as s on s.super_clinic_id = c.super_clinic_id
    where df.delivered_at::date >= '2022-07-01'----------------------------------------------date range - H2 2022
    and df.delivered_at::date <= '2022-12-31'
    and canceled_at is null
    and s.patient_pod_category not in ('Fertility','HIV','Specialty','Dermatology', 'Core')
    and not p.is_covid_antiviral
    and p.product_name not ilike '%Ensure%'
    group by 1,2
)
,jan_data as ( --------------------------------by clinic by product
    select lower(c.clinic_name) as clinic_name,
           c.super_clinic_id,
           count(distinct 
                  case when date_trunc('month',df.delivered_at) = date_trunc('month',pt.created_at) AND
                            p.product_name ilike '%Ozempic%'
                            then pt.merged_into_user_id 
                        else null
                  end) as Ozempic_nps,
           count(distinct IFF(p.product_name ilike '%Ozempic%', df.delivery_id , null )) as Ozempic_fills,
           sum((df.revenue_with_emd_fee +
                coalesce(df.dispensing_revenue_adjustment,0) +
                coalesce(df.progyny_revenue_adjustment,0) -
                coalesce(df.dir_high,0)
                ) - (df.cogs - coalesce(df.emd_rebate_adjustment,0) )
              ) as total_profit,
           sum((df.revenue_with_emd_fee +
                 coalesce(df.dispensing_revenue_adjustment,0) +
                 coalesce(df.progyny_revenue_adjustment,0) -
                 coalesce(df.dir_high,0)
                 )
                ) as total_revenue
    from analytics.core.fct_delivered_fills as df
    left join analytics.core.dim_products as p on p.key = df.product_key
    left join analytics.core.dim_clinics as c on c.key = df.clinic_key
    left join analytics.core.dim_patients as pt on pt.key = df.patient_key
    left join clinic_ta as s on s.super_clinic_id = c.super_clinic_id
    where date_trunc('month',df.delivered_at::date) = '2023-01-01'
    and canceled_at is null
    and s.patient_pod_category not in ('Fertility','HIV','Specialty','Dermatology', 'Core')
    and not p.is_covid_antiviral
    and p.product_name not ilike '%Ensure%'
    group by 1,2
)
,top_meds as (
  select f.*,
         row_number() over (partition by f.clinic_name,f.super_clinic_id order by total_med_fills desc) as med_rank,
         sum(total_med_fills) over (partition by f.clinic_name,f.super_clinic_id) as total_vol,
         round(f.total_med_fills/total_vol,2) as portion_of_vol
  from clinic_fills as f
  qualify med_rank <= 5 ------------------------------------------------------------top X meds
)
,med_agg as ( ------------------------------------------------------parse top meds into columns
  select tm.clinic_name,
         tm.super_clinic_id,
         array_agg(tm.product_name) as top_meds,
         array_agg(tm.portion_of_vol) as vols,
         get(top_meds,0) as no_1_med,
         get(vols,0) as no_1_med_vol,
         get(top_meds,1) as no_2_med,
         get(vols,1) as no_2_med_vol,
         get(top_meds,2) as no_3_med,
         get(vols,2) as no_3_med_vol
  from top_meds as tm
  group by 1,2
)
,MoM_nps as (
     select c.super_clinic_id,
            month(f.created_at) as fiscal_month,
            count(distinct f.user_id) as nps
     from analytics.core.fct_user_funnels as f 
     left join analytics.core.fct_intakes as i on i.key = f.first_intake_key
     left join analytics.core.dim_clinics as c on c.key = i.clinic_key
     left join analytics.core.dim_patients as p on p.key = f.patient_key_d1
     where f.created_at::date >= '2022-11-01'----------------------------------------------date range - H2 2022
     and f.created_at::date <= '2023-01-31'
     and p.is_first_prescription_covid_antiviral <> True 
     group by 1,2
)
,np_pivot as (
    Select super_clinic_id,
           sum(novnp) as nov_np,
            sum(decnp) as dec_np,
            sum(jannpmtd) as jan_np_mtd
    from MoM_nps
        pivot(sum(nps) for fiscal_month in (11,12,1) 
             ) as n (super_clinic_id,novnp,decnp,jannpmtd)
    group by 1

)
,total_gp_by_product_name as ( ------------------------- by product
    select
        pr.product_name,
        pr.brand_vs_generic,
        pr.etc_level_1,
        pr.etc_level_2||' - '||coalesce(pr.etc_level_4, pr.etc_level_3) as description,
        count(distinct fdf.delivery_id) as num_fills,
        avg(
            fdf.revenue_with_emd_fee
            + coalesce(fdf.dispensing_revenue_adjustment, 0)
            + coalesce(fdf.progyny_revenue_adjustment, 0)
            - coalesce(fdf.dir_high, 0)
            - (fdf.cogs - coalesce(fdf.emd_rebate_adjustment, 0))
        ) as avg_gp
        , round(num_fills*avg_gp,2) as total_gp
    from analytics.core.fct_delivered_fills as fdf
    left join analytics.core.dim_products as pr on fdf.product_key = pr.key
    left join analytics.core.dim_patients as dp on fdf.patient_key = dp.key
    where delivered_at >= '2022-07-01'
    and delivered_at <= '2022-12-31'
    and canceled_at is null
    and not(pr.product_name ilike any ('%Wegovy%','%covid%','%Mounjaro%','%Ensure%','%Praluent%','%Repatha%','%Leqvio%','%Prolia%','%Forteo%','%Dupixent%','%Xolair%' ) )
    and not pr.is_covid_antiviral
    and fdf.attribution_therapeutic_area_level_1 in ('Endocrinology', 'Cardiology', 'Pulmonary')
    group by 1,2,3,4
    having num_fills > 10
)
,gp_category as ( ----------------------------------------- pull high performing and low performing meds
    select
        IFF(TOTAL_GP >= 0 and num_fills > 500,'low','other') as cat,
        case when row_number() over (order by total_gp desc) <= 20 then 'High GP Product'
             when row_number() over (order by total_gp asc) <= 20 then 'Negative GP Product'
             when row_number() over (partition by cat order by total_gp asc) <= 20 then 'Low GP Product'
             else 'Other'
         end as med_gp_category
         , *
    from total_gp_by_product_name
    qualify row_number() over (order by total_gp asc) <= 20 or
            row_number() over (order by total_gp desc) <= 20 or 
            (row_number() over (partition by cat order by total_gp asc) <= 20 and cat = 'low')
)
,gp_overview as ( ------------------------------------------------by clinic
  select
      c.super_clinic_id,
      lower(c.clinic_name) as clinic_name,
      s.patient_pod_category as clinic_ta
      , count(distinct IFF( gpc.med_gp_category = 'High GP Product' , fdf.delivery_id ,null) ) as num_high_gp_med_fills
      , count(distinct IFF( gpc.med_gp_category = 'Negative GP Product' , fdf.delivery_id ,null) ) as num_neg_gp_med_fills
      , count(distinct IFF( gpc.med_gp_category = 'Low GP Product' , fdf.delivery_id ,null) ) as num_low_gp_med_fills
      , count(distinct IFF( coalesce(gpc.med_gp_category,'Other') = 'Other' , fdf.delivery_id ,null) ) as num_other_fills
      , count(distinct fdf.delivery_id) as total_deliveries
      , num_high_gp_med_fills/total_deliveries as high_gp_med_fill_pct
      , num_low_gp_med_fills/total_deliveries as low_gp_med_fill_pct
      , num_neg_gp_med_fills/total_deliveries as neg_gp_med_fill_pct
      , num_other_fills/total_deliveries as other_fill_pct
  from analytics.core.fct_delivered_fills as fdf
  left join analytics.core.dim_clinics as c on fdf.clinic_key = c.key
  left join analytics.core.dim_products as pr on fdf.product_key = pr.key
  left join gp_category as gpc on pr.product_name = gpc.product_name
  left join clinic_ta as s on s.super_clinic_id = c.super_clinic_id
  where fdf.delivered_at::date >= '2022-07-01' ---------------------fix date range to H2 2022
  and fdf.delivered_at::date <= '2022-12-31'
  and canceled_at is null
  and not(pr.product_name ilike any ('%Wegovy%','%covid%','%Mounjaro%','%Ensure%','%Praluent%','%Repatha%','%Leqvio%','%Prolia%','%Forteo%','%Dupixent%','%Xolair%' ) )
  and not pr.is_covid_antiviral
  group by 1,2,3
  having  clinic_ta not in ('Fertility','HIV','Specialty','Dermatology','Core')  ---------------filter TA at clinic Level
)
,basket as (
    select c.super_clinic_id,
           count(distinct p.patient_key) as patient_count,
           round(avg(rxg.num_rx_within_14d -
               rxg.num_antiviral_rx_within_14d -
               rxg.num_wegovy_rx_within_14d -
               rxg.num_mounjaro_rx_within_14d
              ),1) as avg_basket_size
    from ANALYTICS.SALES_OPS.INT_NEW_PATIENT_RX_MED_GROUP as rxg
    left join analytics.core.fct_intakes as p on p.user_id = rxg.user_id
    left join analytics.core.dim_clinics as c on c.key = p.clinic_key
    where (rxg.num_rx_within_14d -
           rxg.num_antiviral_rx_within_14d -
           rxg.num_wegovy_rx_within_14d -
           rxg.num_mounjaro_rx_within_14d
         ) > 0   -------------------------exclude standalone specialty patients
    and rxg.first_prescription_created_at::date >= '2022-07-01'
    and rxg.first_prescription_created_at::date <= '2022-12-31'
    group by 1
)
,gp_agg as ( ----------------------------------------------- aggregate fills and calculate
    select cf.clinic_name,
           cf.super_clinic_id,
            cf.state,
           sum(cf.total_profit) as total_gp,
           sum(cf.total_revenue) as total_rev,
           sum(cf.total_med_fills) as total_fills,
           sum(cf.total_patients) as total_patient_cnt,
           sum(cf.neg_gp_fills) as negative_gp_fills,
           sum(cf.np_fills) as np_fills,
           round(negative_gp_fills / total_fills,4) as overall_negative_gp_rate,
           sum(IFF(cf.BRAND_VS_GENERIC = 'Generic Drug',cf.total_med_fills,0) ) as generic_mix,
           round(generic_mix/total_fills,4) as pct_generic,
           listagg(distinct cf.clinic_ta,', ') within group (order by cf.clinic_ta) as therapeutic_area
    from clinic_fills as cf
    group by 1,2,3
)
,sf_data as (
  select cf.super_clinic_id,
         listagg(distinct coalesce( sfa.clinic_ownership_type_c,sfa.ownership_picklist_c), ', ' ) as ownership_type
  from clinic_fills as cf
  left join fivetran.salesforce.account as sfa on coalesce(sfa.scriptdash_id_c::int,
                                                           sfa.wunderbar_id_c::int,
                                                           try_cast(sfa.wb_clinic_id_c as int)
                                                          ) = cf.wunderbar_id::int 
  group by 1
)
,sales as (
    select s.super_clinic_id,
           s.salesforce_owner_name,
           c.partnerships_manager_current as pm, 
           s.market,
           lower(c.clinic_name) as clinic_name,
           listagg(distinct month(s.fiscal_month)||' - '||
                   IFF(s.growth_clinic_funnel_stage_excluding_pts_with_mounjaro_14d is null,'Churned',
                       growth_clinic_funnel_stage_excluding_pts_with_mounjaro_14d),
                   ', ') 
                    within group     
                    (order by month(s.fiscal_month)||' - '||
                              IFF(s.growth_clinic_funnel_stage_excluding_pts_with_mounjaro_14d is null,
                                  'Churned',
                                   growth_clinic_funnel_stage_excluding_pts_with_mounjaro_14d
                                 )
                     ) as stage_hist,
           max(s.growth_clinic_funnel_stage_index_excluding_mounjaro) as highest_stage_reached_int,
           case when highest_stage_reached_int = 4 then 'Emerging'
                when highest_stage_reached_int = 5 then 'Established'
                when highest_stage_reached_int = 6 then 'Partnership'
                else 'Churned'
           end as highest_stage_reached,
           max(s.has_tia) as has_TIA,
           max(s.has_granted_ehr_access) as granted_EHR_access,
           max(s.has_activated_alto_connect) as has_alto_connect
    from ANALYTICS.SALES_OPS.FCT_CLINIC_MONTHLY_FUNNEL_STATUS as s
    left join analytics.core.dim_clinics as c on c.super_clinic_id = s.super_clinic_id
    where year(fiscal_month) = 2022
    and c.first_prescription_created_at::date <= date_trunc('month',s.fiscal_month)
    group by 1,2,3,4,5
)
,stages as (
    select s.super_clinic_id,
           s.fiscal_month,
           lower(s.clinic_name) as clinic_name,
           coalesce(s.growth_clinic_funnel_stage_excluding_pts_with_mounjaro_14d,'none') as stage_label,
          row_number() over (partition by s.super_clinic_id,s.clinic_name order by s.fiscal_month desc) as seq
    from ANALYTICS.SALES_OPS.FCT_CLINIC_MONTHLY_FUNNEL_STATUS as s
    qualify seq = 3  ------------------ pull nov stage 
    order by 1,2
)
 select f.super_clinic_id,
        f.clinic_name, 
        f.therapeutic_area,
        f.state,
        f.total_gp as H2_GP,
        f.total_rev as H2_Rev,
        f.total_patient_cnt as H2_Pts_Served, 
        f.total_fills as H2_Fills,
        f.np_fills,
        f.negative_gp_fills as H2_Neg_GP_Fills,
        f.overall_negative_gp_rate as H2_Neg_GP_Pct_Fills,
        f.pct_generic as H2_Generic_Pct_Fills,
        f.np_fills/f.total_fills as H2_Pct_NP_Fills,
        b.avg_basket_size,
        med.no_1_med,
        med.no_1_med_vol,
        med.no_2_med,
        med.no_2_med_vol,
        med.no_3_med,
        med.no_3_med_vol,
        go.num_high_gp_med_fills,
        go.num_low_gp_med_fills,
        go.num_neg_gp_med_fills,
        go.num_other_fills,
        go.high_gp_med_fill_pct,
        go.low_gp_med_fill_pct,
        go.neg_gp_med_fill_pct,
        go.other_fill_pct,
        s.salesforce_owner_name,
        sf.ownership_type,
        s.pm,
        s.market,
        s.stage_hist,
        s.highest_stage_reached as Highest_Stage_2022,
        st.stage_label as Nov_22_stage,
        s.has_TIA,
        s.granted_EHR_access,
        s.has_alto_connect,
        np.nov_np,
        np.dec_np,
        np.jan_np_mtd,
        mw.total_profit as H2_GP_w_MJ_Weg,
        mw.total_revenue as H2_Rev_w_MJ_Weg,
        j.total_revenue as jan_rev,
        j.total_profit as jan_gp,
        j.Ozempic_fills as jan_Ozempic_fills,
        j.Ozempic_nps as jan_Ozempic_nps
 from gp_agg as f
 left join basket as b on b.super_clinic_id = f.super_clinic_id
 left join med_agg as med on med.clinic_name = f.clinic_name and med.super_clinic_id = f.super_clinic_id
 left join gp_overview as go on go.clinic_name = f.clinic_name and go.super_clinic_id = f.super_clinic_id
 left join sales as s on s.super_clinic_id = f.super_clinic_id and s.clinic_name = f.clinic_name
 left join stages as st on st.super_clinic_id = f.super_clinic_id --and st.clinic_name = f.clinic_name
 left join np_pivot as np on np.super_clinic_id = f.super_clinic_id
 left join sf_data as sf on sf.super_clinic_id = f.super_clinic_id
 left join clinic_money_w_mj_weg as mw on mw.super_clinic_id = f.super_clinic_id and mw.clinic_name = f.clinic_name
 left join jan_data as j on j.super_clinic_id = f.super_clinic_id and j.clinic_name = f.clinic_name
 order by 6 desc


 