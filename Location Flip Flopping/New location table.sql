-- New location table using the three way join 
DROP TABLE IF EXISTS gunjanmohan.location_upd;
CREATE TABLE gunjanmohan.location_upd as (
with loc_table as (select l.*, d.dma_id, d.dma_code
                   from detection.location l
                            left join detection.dma d
                                      on l.fk_dma_id = d.dma_id),
    excel as (
        select *  from gunjanmohan.new_zip_dma_mapping d
         left join gunjanmohan.zip_county_dma_mapping z
         on LPAD(d.zip, 5, '0')= LPAD(z.zip_code, 5, '0')
    ),

all_joined as (

select   l.location_id, l.fk_dma_id, l.country_code, l.region, l.iso_state, l.city,l.timezone,l.fk_dma_reported,
        e.city as ecity, e.state as estate, e.county as ecounty, e.dma as edma,
        t.city as tcity, t.state as tstate, t.dma_name as tdma,
       --l.zipcode,
        CASE WHEN LENGTH(l.zipcode) > 5 THEN replace(l.zipcode, '-', '') else l.zipcode
        END AS zipcode,
       l.dma_code,
       e.zip as ezip, e.dma_code as edma_code,
       t.zipcode as tzip, t.dma_code as tdma_code
from  (select distinct fk_location_id
        from detection.tv_geolocation
        where create_timestamp between DATEADD(month, -12, GETDATE()) and GETDATE()
        ) tv
     left join  loc_table l
    on tv.fk_location_id = l.location_id
left join excel e
on LPAD(l.zipcode, 5, '0')= LPAD(e.zip, 5, '0') AND LENGTH(l.zipcode) <= 5 AND LENGTH(e.zip) <= 5
left join detection.dma_zipcode t
on LPAD(l.zipcode, 5, '0')= LPAD(t.zipcode, 5, '0') AND LENGTH(l.zipcode) <= 5 AND LENGTH(t.zipcode) <= 5)

select distinct location_id, region,
                case when dma_code is null and (edma_code is not null or tdma_code is not null) and (country_code = 'US') then coalesce(ecity, tcity)
                    else city end as city,
                zipcode,
                fk_dma_id,
                case when dma_code is null and (edma_code is not null or tdma_code is not null)   and (country_code = 'US') then coalesce(estate, tstate)
                    else iso_state end as iso_state,
                timezone,
                country_code,
                fk_dma_reported,
                case when dma_code is null and (edma_code is not null or tdma_code is not null)   and (country_code = 'US')
                    then coalesce(CAST(edma_code AS VARCHAR) , CAST(tdma_code AS VARCHAR))
                    else CAST (dma_code as CHAR ) end as dma_code,
                coalesce(edma, tdma) as assigned_dma_name
from  all_joined
    a)