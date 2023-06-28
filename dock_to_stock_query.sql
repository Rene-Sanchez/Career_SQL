with
--                AND (item_number not like '1%' --belts
--                  OR item_number not like '3%' --accessories
--                  OR item_number not like '8%') --neckwear
append_prev_nxt_and_harvest_returns as (
    SELECT end_tran_date,
           tran_log_id,
           hu_id,
           tran_type,
           description,
           LAG(tran_type, 1, 0) OVER (PARTITION BY hu_id ORDER BY tran_log_id) AS Previous_Location,
           LEAD(tran_type, 1, 0) OVER (PARTITION BY hu_id ORDER BY tran_log_id) AS Next_Location
    FROM fivetran.highjump_replica_dbo.t_tran_log
    WHERE end_tran_date >= '2021-06-01'
      AND tran_type in ('526', '216', '214') --,'523' sometimes contains returns
)

, pruning_data_1 as ( -- ensures that event sets start with a return and itemizes sets
    SELECT
    row_number() over (partition BY hu_id ORDER BY tran_log_id) AS rn ,
    tran_log_id,
    end_tran_date,
    hu_id,
    tran_type,
    description,
--     previous_location,
--     Next_Location,
    CASE
       WHEN (Previous_Location = 0 AND tran_type in ('216', '214')) -- remove stub putaway events
                or  (Previous_Location = 0 AND Next_Location = 0) then 'Delete' -- removes events not part of set
       ELSE null
       END AS Delete_
    FROM append_prev_nxt_and_harvest_returns
    WHERE previous_location <> tran_type -- removes redundant scans
--     and  HU_ID = '10163803'
     and Delete_ is null
)

,reappend_prev_nxt as (
    SELECT
    rn,
    tran_log_id,
    end_tran_date,
    hu_id,
    description,
    LAG(rn, 1, 0) OVER (PARTITION BY hu_id ORDER BY tran_log_id)  AS Previous_item,
    LEAD(rn, 1, 0) OVER (PARTITION BY hu_id ORDER BY tran_log_id) AS Next_item,
    CASE
        WHEN (right(RN, 1) in (1, 3, 5, 7, 9) AND (Previous_item = Next_item)) -- drop redundant scans
            or  (right(RN, 1) in (1, 3, 5, 7, 9) AND (Next_item = 0)) -- drop returns with no next location
             then 'Delete'
    ELSE null
    END AS Delete_Row
    from pruning_data_1
)

,generate_return_date as (
    SELECT end_tran_date AS Date,
           hu_id AS Barcode,
           Description,
           Delete_Row,
           CASE
               WHEN description in ('WaterBug Putaway', 'Shoe Putaway')
                   then LAG(end_tran_date, 1) OVER (PARTITION BY hu_id ORDER BY end_tran_date)
               ELSE null
               END AS Returned_Date
    FROM reappend_prev_nxt
    WHERE Delete_Row is null
)

SELECT
Date,
Barcode,
Description,
Returned_Date,
DATEDIFF(day, Returned_Date, Date) D2S
FROM generate_return_date
WHERE description <> 'Returns'
-- and Barcode in ('60394775','60237072')