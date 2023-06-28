-------------------item level replacements
with
shows as (
    select USER_ID,
           EVENT_ID,
           SHOWROOM_GROUP_NAME,
           SHOWROOM_APPOINTMENT_DATE,
           row_number() over (partition by USER_ID,EVENT_ID order by SHOWROOM_APPOINTMENT_DATE desc) as oc
    from BI.TABLEAU_SHOWROOM_APPT_DATASET as s
    where s.CANCELLED_CNT < 1 and s.NO_SHOW_CNT < 1
    group by 1,2,3,4
    qualify oc = 1
)
,parents as (
      SELECT i.USER_ID,
             i.EVENT_ID,
             i.EVENT_DATE,
             i.ORDER_ID,
             s.HU_ID,
             st.FIFO_DATE,
             datediff(day, st.FIFO_DATE,current_date) as age,
             IFF(sh.SHOWROOM_APPOINTMENT_DATE is null,false,true) as HAS_SHOWROOM_APPOINTMENT,
             sh.SHOWROOM_GROUP_NAME,
             o.ORDER_HAD_SKU_SELLOUT,
             i.TAILOR_COMMENTS,
             iff(TAILOR_COMMENTS is not null,true,false) as has_tailoring,
             i.SKU_CODE,
             i.SKU_SIZE,
             i.SKU_FIT,
             i.SKU_LENGTH,
             i.SIZE_CHECK_ID as og_key,
             i.SHIPMENT_DATE,
             coalesce(v.VENDOR_LOT_NUM, i.VERSION_NAME) as VERSION_NAME,
             coalesce(v.LOT_NUMBER,i.COMPATIBILITY_GROUP_NAME) as COMPATIBILITY_GROUP_NAME,
             i.PRODUCT_CATEGORY_NAME,
             i.SHIPMENT_TYPE_NAME,
             i.PRODUCT_STYLE_NAME,
             i.PRODUCT_STYLE_CODE,
             i.IS_DELIVERY_CANCELED as delivery_unsuccessful,
             i.ORDER_ID||i.PRODUCT_STYLE_CODE||COALESCE(s.HU_ID,'') as UID,
             row_number() over (partition by i.ORDER_ID||i.PRODUCT_STYLE_CODE order by i.SHIPMENT_DATE) as cleaner
      FROM DW.DA_DIM.F_SHIPMENTS_ITEMS as i
      left join DA_DIM.D_ORDER as o on o.ORDER_ID = i.ORDER_ID
      left join FIVETRAN.HIGHJUMP_DBO.T_AL_HOST_SHIPMENT_DETAIL as s on s.ORDER_NUMBER = i.SHIPPING_REFERENCE_CODE
                                                                             and s.ITEM_NUMBER = i.SKU_CODE
      left join FIVETRAN.HIGHJUMP_REPLICA_DBO.T_STORED_ITEM as st on st.HU_ID = s.HU_ID
      left join DA_DIM.D_EVENT as e on e.EVENT_ID = i.EVENT_ID
      left join FIVETRAN.HIGHJUMP_REPLICA_DBO.T_RECEIPT as v on s.HU_ID = v.hu_id::varchar
      left join shows as sh on sh.EVENT_ID = i.EVENT_ID
                            and sh.USER_ID = i.USER_ID
      where i.IS_REPLACEMENT = false
     and i.SHIPMENT_TYPE_NAME not like '%HTO%'
     and st._FIVETRAN_DELETED = false
     and i.IS_SHIPMENT_VOIDED = false
     and i.IS_HOME_TRY_ON = false
     and e.EVENT_DATE < current_date
    and year(e.EVENT_DATE) > 2018
 )
,child as (
    SELECT i.USER_ID,
           EVENT_ID,
           ORDER_ID,
           i.SHIPMENT_DATE,
           SHIPPING_REFERENCE_CODE,
           PARENT_ORDER_ID,
           i.SKU_CODE,
           i.SKU_SIZE,
           SKU_LENGTH,
           SKU_FIT,
           i.TAILOR_COMMENTS as replace_tailoring,
           i.SKU_CATEGORY_NAME,
           PRODUCT_STYLE_NAME,
           i.PRODUCT_STYLE_CODE,
           coalesce(v.VENDOR_LOT_NUM, i.VERSION_NAME) as VERSION_NAME,
            coalesce(v.LOT_NUMBER,i.COMPATIBILITY_GROUP_NAME) as COMPATIBILITY_GROUP_NAME,
           row_number() over (partition by PARENT_ORDER_ID,PRODUCT_STYLE_CODE order by ORDER_ID asc) as occur,
            lead(SKU_SIZE) over( partition by PARENT_ORDER_ID, PRODUCT_STYLE_CODE order by SHIPMENT_DATE) as nxt_replace_size ,
           lead(SKU_LENGTH) over( partition by PARENT_ORDER_ID, PRODUCT_STYLE_CODE order by SHIPMENT_DATE) as nxt_replace_length ,
           PARENT_ORDER_ID||i.PRODUCT_STYLE_CODE||s.HU_ID as UID
  FROM DW.DA_DIM.F_SHIPMENTS_ITEMS as i
  left join FIVETRAN.HIGHJUMP_DBO.T_AL_HOST_SHIPMENT_DETAIL as s on s.ORDER_NUMBER = i.SHIPPING_REFERENCE_CODE
                                                                             and s.ITEM_NUMBER = i.SKU_CODE
  left join FIVETRAN.HIGHJUMP_REPLICA_DBO.T_STORED_ITEM as st on st.HU_ID = s.HU_ID and st._FIVETRAN_DELETED = false
  left join FIVETRAN.HIGHJUMP_REPLICA_DBO.T_RECEIPT as v on s.HU_ID = v.hu_id::varchar
  where i.IS_REPLACEMENT = true
  and i.IS_SHIPMENT_VOIDED = false
    and i.IS_HOME_TRY_ON = false
    qualify occur = 1  ---- taking first replacement
 )
,showroom_assoc as (
select s.ASSOCIATE_USER_ID,
       u.USER_FULL_NAME,
       u.USER_EMAIL,
       'showroom Associate'
from DA_DIM.F_SHOWROOM_APPOINTMENTS as s
left join DA_DIM.D_USER as u on u.USER_ID = s.ASSOCIATE_USER_ID
where ASSOCIATE_USER_ID <> -1001
and u.USER_FULL_NAME <> '(Missing)'
group by 1,2,3,4
)
,cx_assoc as (
    select USER_ID,
           EMPLOYEE_FULL_NAME,
           trim(EMPLOYEE_SITE) as site,
           coalesce(IFF(trim(EMPLOYEE_GROUP) like 'CX',EMPLOYEE_GROUP||'-'||site,EMPLOYEE_GROUP ), 'CX-'||EMPLOYEE_SITE) as employee_group
    from DA_DIM.D_CUSTOMER_CARE
)
,surveys as (
    select u.USER_ID as survey_user,
           SIZE_SURVEY_ID,
           u.SIZE_CHECK_ID as prime_key,
           SIZE_CHECK_REASON_DESC,
           PRODUCT_CATEGORY_NAME as survey_product_category,
           u.IS_AUTO_SIZE_CHECKED,
           u.PASSED_AUTO_SIZE_CHECKED,
           PREDICTED_SIZE,
           SURVEY_SIZE,
           CHECKED_SIZE,
           IFF(SIZE_CHECK_REASON_DESC in ('Fit Specialist/TBT Admin','Customer Request'),
               case
                   when s.ASSOCIATE_USER_ID is not null then 'Showroom Appointment'
                   when c.USER_ID is not null then c.employee_group
                   else SIZE_CHECK_REASON_DESC
                   end,
               SIZE_CHECK_REASON_DESC
               )  as survey_reason,
           u.SIZE_CHECK_TS
    from DA_DIM.D_SIZE_CHECK as u
    left join showroom_assoc as s on s.ASSOCIATE_USER_ID = u.ADMIN_USER_ID
    left join cx_assoc as c on c.USER_ID = u.ADMIN_USER_ID
    where u.PRODUCT_CATEGORY_NAME is not null
    and not(ADMIN_USER_ID in (1159722,313128) and SIZE_CHECK_REASON_DESC like 'Fit Specialist/TBT Admin')
      and u.SIZE_CHECK_REASON_DESC not ilike '%replace%'
)
,orders as (
    select o.ORDER_ID,
           o.EVENT_ID,
           o.USER_ID,
           o.EVENT_DATE,
           o.SHIPMENT_DATE,
           o.PRODUCT_CATEGORY_NAME,
           o.og_key
    from parents as o
)
,joins as (
    select s.*,
           o.EVENT_DATE as survey_mapped_event_date,
           o.ORDER_ID as survey_mapped_order,
           row_number()
                   over (partition by s.survey_user,survey_product_category,ORDER_ID order by SIZE_CHECK_TS desc) as occur
    from surveys as s
    left join orders as o on s.prime_key = o.og_key
    where  EVENT_DATE is not null
    qualify occur = 1
)
,form_tran as (
    select ORDER_ID as replacement_form_orders,
           p.PRODUCT_CATEGORY_NAME as f_PRODUCT_CATEGORY_NAME,
           TICKET_CREATED_DATE,
           TICKET_ID,
           REQUESTED_LENGTH,
           case when p.PRODUCT_CATEGORY_NAME like 'Jackets' then get(FIT_DESCRIPTION,'jacket_body')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Pants' then get(FIT_DESCRIPTION,'seat')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Shirts' then get(FIT_DESCRIPTION,'shirt_body')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Vests' then get(FIT_DESCRIPTION,'vest_body')::varchar
               end  as item_body,
           case when p.PRODUCT_CATEGORY_NAME like 'Jackets' then get(FIT_DESCRIPTION,'jacket_sleeve')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Pants' then get(FIT_DESCRIPTION,'inseam')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Shirts' then get(FIT_DESCRIPTION,'shirt_sleeve')::varchar
               end  as item_inseams,
           case when p.PRODUCT_CATEGORY_NAME like 'Jackets' then get(FIT_DESCRIPTION,'jacket_length')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Pants' then get(FIT_DESCRIPTION,'waist')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Shirts' then get(FIT_DESCRIPTION,'neck')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Vests' then get(FIT_DESCRIPTION,'vest_length')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Shoes' then get(FIT_DESCRIPTION,'undefined')::varchar
                when p.PRODUCT_CATEGORY_NAME like 'Belts' then get(FIT_DESCRIPTION,'belt')::varchar
               end as item_sizing,
           IS_FIT_REPLACEMENT ,
           IS_ITEM_MISSING_REPLACEMENT,
           IS_DAMAGED_REPLACEMENT ,
           IS_WRONG_ITEM_REPLACEMENT
    from _STAGE.ZENDESK_REPLACEMENT_FORM as r
    left join DA_DIM.D_PRODUCT_SET as p on p.PRODUCT_SET_ID = r.PRODUCT_SET_ID
)
,forms as (
    select replacement_form_orders,
           f_PRODUCT_CATEGORY_NAME,
           count(distinct TICKET_ID) as forms,
           listagg(distinct IFF(item_body like 'Just right','',','||item_body)) as filtered_body ,
           listagg(distinct IFF(item_inseams like 'Just right','',','||item_inseams) ) as filtered_inseams ,
           listagg(distinct IFF(item_sizing like 'Just right','',','||item_sizing)) as filtered_sizing ,
           max(IS_FIT_REPLACEMENT) as fit_issue,
           max(IS_ITEM_MISSING_REPLACEMENT) as item_missing,
           max(IS_DAMAGED_REPLACEMENT) as item_damaged,
           max(IS_WRONG_ITEM_REPLACEMENT) as item_wrong
    from form_tran as r
    group by 1,2
)
,agg as (
select p.*,
       row_number() over (partition by p.uid order by p.SHIPMENT_DATE) as oc,
       c.ORDER_ID as replacement,
       c.SHIPMENT_DATE as replacement_shipment_date,
       c.SKU_CODE as replacement_SKU_code,
       c.SKU_SIZE as replacement_SKU_size,
       c.SKU_LENGTH as replacement_SKU_length,
       c.SKU_FIT as replacement_SKU_fit,
        c.nxt_replace_length,
       c.nxt_replace_size,
       c.replace_tailoring,
       c.VERSION_NAME as replacement_version,
       c.COMPATIBILITY_GROUP_NAME as replacement_cg,
       (try_cast(p.SKU_SIZE as int) - try_cast(c.SKU_SIZE as int))*-1 as Size_num_change,
       (try_cast(p.SKU_LENGTH as int) - try_cast(c.SKU_LENGTH as int))*-1 as Length_num_change,
        p.FIFO_DATE as item_fifo_date,
        p.age as item_age,
        iff(c.ORDER_ID is not null,true,false)  as has_replacement,
        IFF(c.SKU_SIZE != p.SKU_SIZE,true,false)  as size_change,
        IFF(c.SKU_LENGTH != p.SKU_LENGTH,true,false)  as length_change,
        IFF(c.SKU_FIT != p.SKU_FIT,true,false)  as fit_change,
        IFF(c.SKU_CODE = p.SKU_CODE and c.SKU_CODE is not null ,true,false)  as no_change_replacement
from parents as p
left join child as c on c.PARENT_ORDER_ID = p.ORDER_ID and c.PRODUCT_STYLE_CODE = p.PRODUCT_STYLE_CODE and cleaner = 1
qualify oc = 1
order by p.uid
)
,pos as (
    select a.HU_ID,
       a.ITEM_NUMBER,
       a.ASN_NUMBER,
       a.LOT_NUMBER,
       a.PO_NUMBER,
       m.VENDOR_CODE,
       m.DELIVERY_DATE,
        SERIAL_NUMBER as RFID,
        v.VENDOR_NAME,
       row_number() over (partition by HU_ID order by DELIVERY_DATE desc) as oc
from FIVETRAN.HIGHJUMP_REPLICA_DBO.T_ASN_DETAIL as a
left join FIVETRAN.HIGHJUMP_REPLICA_DBO.T_ASN_MASTER as m on m.ASN_NUMBER = a.ASN_NUMBER
left join _STAGE.PURCHASE_ORDERS as v on v.PURCHASE_ID = a.PO_NUMBER and m.VENDOR_CODE = v.VENDOR_ID and a.ITEM_NUMBER = v.SKU_CODE
qualify oc = 1
order by CREATE_DATE asc
)
,swaps as (
    SELECT item_number,
       lot_number,
       hu_id,
       hu_id_2,
       control_number,
       control_number_2,
       end_tran_date,
        TRAN_LOG_ID
from FIVETRAN.HIGHJUMP_REPLICA_DBO.T_TRAN_LOG
where tran_type = '841'

union all

SELECT item_number,
       lot_number,
       hu_id,
       hu_id_2,
       control_number,
       control_number_2,
       end_tran_date,
       TRAN_LOG_ID
from FIVETRAN.HIGHJUMP_REPLICA_ARCH_AADUTILUSER.T_TRAN_LOG_HIST
where tran_type = '841'
)
,swap_clean as (
    select HU_ID as barcode,
           HU_ID_2,
           END_TRAN_DATE,
           TRAN_LOG_ID
    from swaps
where HU_ID_2 != HU_ID
order by 1,3
)
,swap_finder as (
    select
           least(s.END_TRAN_DATE,
            coalesce(s1.END_TRAN_DATE,'01/01/2999'),
           coalesce(s2.END_TRAN_DATE,'01/01/2999'),
           coalesce(s3.END_TRAN_DATE,'01/01/2999'),
           coalesce(s4.END_TRAN_DATE,'01/01/2999'),
           coalesce(s5.END_TRAN_DATE,'01/01/2999'),
            coalesce(s6.END_TRAN_DATE,'01/01/2999')
                ) as first_tran_date,
           least(s.TRAN_LOG_ID,
            coalesce(s1.TRAN_LOG_ID,99999999),
           coalesce(s2.TRAN_LOG_ID,99999999),
           coalesce(s3.TRAN_LOG_ID,99999999),
           coalesce(s4.TRAN_LOG_ID,99999999),
           coalesce(s5.TRAN_LOG_ID,99999999),
            coalesce(s6.TRAN_LOG_ID,99999999)
                ) as first_tran_id,
           GREATEST(s.TRAN_LOG_ID,
            coalesce(s1.TRAN_LOG_ID,000000000),
           coalesce(s2.TRAN_LOG_ID,000000000),
           coalesce(s3.TRAN_LOG_ID,000000000),
           coalesce(s4.TRAN_LOG_ID,000000000),
           coalesce(s5.TRAN_LOG_ID,000000000),
            coalesce(s6.TRAN_LOG_ID,000000000)
                ) as last_tran_id,
           row_number() over (order by first_tran_id) as keys
    from swap_clean as s
    left join swap_clean as s1 on s1.barcode = s.HU_ID_2 and s1.barcode != s.barcode
    left join swap_clean as s2 on s2.barcode = s1.HU_ID_2 and s2.barcode != s.barcode and s2.barcode != s1.barcode
    left join swap_clean as s3 on s3.barcode = s2.HU_ID_2 and s3.barcode != s.barcode and s3.barcode != s1.barcode
                                      and s3.barcode != s2.barcode
    left join swap_clean as s4 on s4.barcode = s3.HU_ID_2 and s4.barcode != s.barcode and s4.barcode != s1.barcode
                                      and s4.barcode != s2.barcode and s4.barcode != s3.barcode
    left join swap_clean as s5 on s5.barcode = s4.HU_ID_2 and s5.barcode != s.barcode and s5.barcode != s1.barcode
                                      and s5.barcode != s2.barcode and s5.barcode != s3.barcode and s5.barcode != s4.barcode
    left join swap_clean as s6 on s6.barcode = s5.HU_ID_2 and s6.barcode != s.barcode and s6.barcode != s1.barcode
                                      and s6.barcode != s2.barcode and s6.barcode != s3.barcode and s6.barcode != s4.barcode
                                      and s6.barcode != s5.barcode
)
,last_hu as (
    select s.TRAN_LOG_ID,
           s.barcode,
           s.HU_ID_2,
           s.END_TRAN_DATE,
           f.keys,
           f.last_tran_id
    from swap_clean as s
             inner join swap_finder as f on f.last_tran_id = s.TRAN_LOG_ID
    group by 1, 2, 3, 4, 5, 6
)
,first_hu as (
    select s.TRAN_LOG_ID,
           s.barcode,
           s.HU_ID_2,
           s.END_TRAN_DATE,
           f.first_tran_id,
           f.keys
    from swap_clean as s
             inner join swap_finder as f on f.first_tran_id = s.TRAN_LOG_ID
    group by 1, 2, 3, 4, 5, 6
)
,swap_fin as (
    select
           l.HU_ID_2 as most_recent_change,
           f.barcode as orignial,
           row_number() over (partition by most_recent_change order by l.END_TRAN_DATE desc) as oc
    from last_hu as l
    left join first_hu as f on f.keys = l.keys
    qualify oc = 1
)
,version_pref as (
    select USER_ID,
       PRODUCT_CATEGORY_NAME,
       listagg(SHOWROOM_PREFERRED_VERSION_NAME,',') as showroom_version
from DA_DIM.D_USER_VERSION_PREFERENCE_XRF
where SHOWROOM_PREFERRED_VERSION_NAME = 'G'
group by 1,2
)
,change_reason as (
    select a.UID,
           a.SKU_CODE,
           a.PRODUCT_STYLE_NAME,
           max(case when z.TICKET_COMMENTS ilike '%damage%'
                     or z.TICKET_COMMENTS ilike '%tear%' then 'Damaged'
                when try_cast(a.SKU_LENGTH as int) - try_cast(f.REQUESTED_LENGTH as int) = 1
                     and replace_tailoring like '%1%'
                    then 'tailored to size'
                when try_cast(a.SKU_LENGTH as int) - try_cast(f.REQUESTED_LENGTH as int) = 2
                     and replace_tailoring like '%2%'
                    then 'tailored to size'
                when replace_tailoring is not null then 'tailored to size'
                when z.TICKET_COMMENTS like '%Jacket Sleeve Tailoring%' then 'tailored to size'
                when z.TICKET_COMMENTS ilike '%missing%'
                    or z.TICKET_COMMENTS ilike '%arrived%'
                    or z.TICKET_COMMENTS ilike '%lost%'
                    or z.TICKET_COMMENTS ilike '%not delivered%'
                    or z.TICKET_COMMENTS ilike '%not received%'
                    or z.TICKET_COMMENTS ilike '%didn''t received%'
                    then 'Missing Item'
                when z.TICKET_COMMENTS ilike '%wrong%' then 'Wrong Item'
                when z.TICKET_COMMENTS ilike '%returned to the sender%'
                     or z.TICKET_COMMENTS ilike '%returned to sender%'
                     or z.TICKET_COMMENTS ilike 'WAS RETURNED'
                     or z.TICKET_COMMENTS ilike '%delivered%' then 'delivery issue'
                when a.PRODUCT_STYLE_NAME ilike '%jacket%'
                     and z.TICKET_COMMENTS ilike '%wrinkle%' then 'Wrinkled Item'
                when z.TICKET_COMMENTS ilike '%stolen%' then 'stolen'
                when a.PRODUCT_STYLE_NAME ilike '%shirt%'
                     and z.TICKET_COMMENTS ilike '%extend%' then 'Extenders sent for shirt'
               when nxt_replace_size != replacement_SKU_size
                    or nxt_replace_length != replacement_SKU_length then 'Wrong Replacement Sent'
               when a.PRODUCT_STYLE_NAME ilike '%pant%'
                     and z.TICKET_COMMENTS ilike '%jacket%'
                    and z.TICKET_COMMENTS not ilike '%pant%'
                    then 'pant-jacket bundle replacement'
                when a.PRODUCT_STYLE_NAME ilike '%jacket%'
                     and z.TICKET_COMMENTS ilike '%pant%'
                    and z.TICKET_COMMENTS not ilike '%jacket%'
                    then 'pant-jacket bundle replacement'
               end) as reason
    from agg as a
    left join DA_DIM.D_ZENDESK_TICKETS as z on z.ORDER_ID = a.ORDER_ID and z.USER_ID = a.USER_ID
    left join form_tran as f on f.replacement_form_orders = a.ORDER_ID and a.PRODUCT_CATEGORY_NAME = f.f_PRODUCT_CATEGORY_NAME
    where no_change_replacement = true
    and REQUEST_TYPE ilike '%replace%'
    and z.TICKET_COMMENTS not ilike '%UJET%'
    group by a.UID,
             a.SKU_CODE,
             a.PRODUCT_STYLE_NAME
)
,bar_finder as (
    select a.*,
           coalesce(b.orignial,HU_ID) as barcode_finder,
           f.replacement_form_orders,
           f.filtered_body ,
           f.filtered_inseams ,
           f.filtered_sizing ,
           f.fit_issue,
           f.item_missing,
           f.item_damaged,
           f.item_wrong,
           j.SIZE_CHECK_REASON_DESC,
           j.IS_AUTO_SIZE_CHECKED,
           j.PASSED_AUTO_SIZE_CHECKED,
           j.PREDICTED_SIZE,
           j.SURVEY_SIZE,
           j.CHECKED_SIZE,
           j. survey_reason,
           j.SIZE_CHECK_TS,
           c.reason as no_change_reason
    from agg as a
    left join swap_fin as b on b.most_recent_change = a.HU_ID
    left join change_reason as c on a.UID = c.UID
    left join forms as f on f.replacement_form_orders = a.ORDER_ID
                        and f.f_PRODUCT_CATEGORY_NAME = a.PRODUCT_CATEGORY_NAME
    left join joins as j on j.survey_mapped_order = a.ORDER_ID
                        and j.survey_product_category = a.PRODUCT_CATEGORY_NAME
)
select a.*,
       v.showroom_version,
       b.PO_NUMBER,
       case when b.VENDOR_CODE = 'V01295'
                         and a.PRODUCT_CATEGORY_NAME not ilike '%Jackets%'
               then 'Tag'
                when b.VENDOR_CODE = 'V03707' then 'LEVER STYLE LIMITED'
                when b.VENDOR_CODE = 'V03046'
                         and a.PRODUCT_CATEGORY_NAME not ilike '%studs%'
                    then 'MANE ENTERPRISES'
                when b.VENDOR_CODE = 'V03405' then 'D''Lord''s Footwear & Fashions Pvt Ltd'
                when b.VENDOR_CODE = 'V03530' then 'SKS GLOBAL PRIVATE LIMITED'
                when b.VENDOR_CODE = 'V02727' then 'Pinnacle Brand Group, Inc.'
                when b.VENDOR_CODE = 'V02243'
                         and a.PRODUCT_CATEGORY_NAME not ilike '%Jackets%'
                         and a.PRODUCT_CATEGORY_NAME not ilike '%shirt%'
                        and a.PRODUCT_CATEGORY_NAME not ilike '%pant%'
                    then 'Shanghai Silk Group Co., LTD'
                when b.VENDOR_CODE = 'V01054'
                         and a.PRODUCT_CATEGORY_NAME not ilike '%studs%'
                         and a.PRODUCT_CATEGORY_NAME not ilike '%shirt%'
                    then 'Pt Trisco Tailored Apparel Manufacturing'
               else ''
               end as vendor_names
from bar_finder as a
left join pos as b on b.HU_ID = a.barcode_finder
left join version_pref as v on v.PRODUCT_CATEGORY_NAME = a.PRODUCT_CATEGORY_NAME and a.USER_ID = v.USER_ID