-- Create the new smoothing table with the tv id, week start and week end and the winner smoothed DMA.
DROP TABLE IF EXISTS gunjanmohan.dma_mapping_smoothing_old;
CREATE TABLE gunjanmohan.dma_mapping_smoothing_old AS (
-- This gets the week end and start dates between 1 to 31 Mar
with weeks  AS (
select distinct date_trunc('week', dates) AS week_start,
       DATE_TRUNC('week', dates) + INTERVAL '1 week' - INTERVAL '1 sec' as week_end
       from public.dates_updated
where dates between '2023-03-01' and '2023-03-31'
order by 1),


mar_tv_geolocation as (
select tloc.fk_location_id, loc.zipcode, loc.fk_dma_id, loc.region,
       --loc.dma_code, loc.assigned_dma_name,
       tloc.fk_tvid, tloc.create_timestamp, tloc.next_create_timestamp,
       -- update the next create timestamp for Tvs with 2100
       case when next_create_timestamp = '2100-01-01 00:00:00.000000'
            then '2023-03-31 00:00:00.000000'
            else next_create_timestamp end as next_create_timestamp_upd
        from
        detection.tv_geolocation tloc
        left join detection.location loc
        on tloc.fk_location_id = loc.location_id
        where create_timestamp between  '2023-03-01' and '2023-03-31'
)
select * from (
select *,  row_number() over (partition by fk_tvid, week_start, week_end order by time_spent desc) as r
from ( select fk_tvid,  DATE_TRUNC('week', start_time) AS week_start,
DATE_TRUNC('week', start_time) + INTERVAL '1 week' - INTERVAL '1 sec' as week_end,
fk_dma_id,
zipcode, sum(minutes) as time_spent
from (
  SELECT
    fk_location_id, fk_tvid,
    -- To fix TVs with create and next_create spacing across more than 1 week
    CASE
      WHEN create_timestamp >= week_start THEN create_timestamp
      ELSE week_start
    END AS start_time,
    CASE
      WHEN next_create_timestamp_upd <= week_end THEN next_create_timestamp_upd
      ELSE week_end
    END AS end_time,
    DATEDIFF(minute, start_time, end_time ) as minutes, fk_dma_id,
    zipcode
  from mar_tv_geolocation
  JOIN weeks
    ON create_timestamp <= week_end
    AND next_create_timestamp >= week_start
order by 2,3,4) a
    group by 1,2,3,4,5
             ) b) c
where r = 1
order by 1,2,3
)

-- Join the tv_geolocation table back with the mapping table above to smooth over the DMA's 
DROP TABLE IF EXISTS gunjanmohan.tv_geolocation_smooth_old;
CREATE TABLE  gunjanmohan.tv_geolocation_smooth_old AS (
SELECT
  tloc.fk_location_id, tloc.fk_tvid,
  CASE
    WHEN tloc.create_timestamp >= map.week_start AND tloc.create_timestamp <= map.week_end
    THEN tloc.create_timestamp
    ELSE map.week_start
  END AS create_timestamp,
  CASE
    WHEN tloc.next_create_timestamp >= map.week_start AND tloc.next_create_timestamp <= map.week_end
    THEN tloc.next_create_timestamp
    ELSE map.week_end
  END AS next_create_timestamp,
 map.week_start, map.week_end, map.time_spent as smooth_dma_time,  map.fk_dma_id as smooth_dma,
 map.zipcode as smooth_zip
FROM detection.tv_geolocation tloc
  left JOIN gunjanmohan.dma_mapping_smoothing_old map ON tloc.fk_tvid = map.fk_tvid
WHERE tloc.create_timestamp <= map.week_end
  AND tloc.next_create_timestamp >= map.week_start
  and create_timestamp between '2023-03-01' and '2023-03-31'
order by 2,3,4
)


--Get the rate of flip flop code
select count(distinct fk_tvid) as total_tvs,
       count (distinct case when dma_sflag = 1 then fk_tvid else null end ) as switched_dma,
       (switched_dma *1.0)/total_tvs as perc_dma_switch
        from (
select *,
       case when fk_dma_id <> next_dma then 1 else 0 end as dma_sflag
from (
select fk_tvid, create_timestamp, next_create_timestamp, smooth_dma as fk_dma_id,
       lead(smooth_dma) over (partition by fk_tvid order by create_timestamp) as next_dma,
       lead(create_timestamp) over (partition by fk_tvid order by create_timestamp) as next_time,
       DATEDIFF(minute, create_timestamp, next_create_timestamp) AS minutes_diff
        from gunjanmohan.tv_geolocation_smooth_old
        order by 1,2,3 )a
order by 1,2,3
    ) b
