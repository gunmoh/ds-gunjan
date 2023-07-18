
-- Create a new table with just the age and gender demographics and filtering for the latest match date 
DROP TABLE IF EXISTS gunjanmohan.agedemo_bucket;
CREATE TABLE gunjanmohan.agedemo_bucket AS (
select * from (select tv.tvid,
                      tv.token,
                      demo_male_18_24,
                      demo_male_25_34,
                      demo_male_35_44,
                      demo_male_45_54,
                      demo_male_55_64,
                      demo_male_65_999,
                      demo_male_18_29,
                      demo_male_30_39,
                      demo_male_40_49,
                      demo_male_50_59,
                      demo_male_60_69,
                      demo_male_70_999,
                      demo_male_21_plus,
                      demo_female_18_24,
                      demo_female_25_34,
                      demo_female_35_44,
                      demo_female_45_54,
                      demo_female_55_64,
                      demo_female_65_999,
                      demo_female_18_29,
                      demo_female_30_39,
                      demo_female_40_49,
                      demo_female_50_59,
                      demo_female_60_69,
                      demo_female_70_999,
                      demo_female_21_plus,
                      --match_date,
                      row_number() over (partition by token order by joined_date desc) as r
               from detection.experian_demography_curr  ed
                           join detection.tv
                                on tv.tvid = ed.tvid
              where (income_0_35_hh + income_35_45_hh + income_45_55_hh + income_55_70_hh + income_70_85_hh
                                +income_85_100_hh+income_100_125_hh+income_125_150_hh+income_150_200_hh+income_200_plus_hh) = 1
              ) a
              where r = 1)


-- Create the new buckets 
DROP TABLE IF EXISTS gunjanmohan.agedemo_bucket_new;
CREATE TABLE gunjanmohan.agedemo_bucket_new AS (
SELECT tvid, token,
  (demo_male_18_29 + demo_male_30_39 + demo_male_40_49 +demo_male_50_59 +demo_male_60_69 +demo_male_70_999) - demo_male_21_plus as demo_male_18_20,
  demo_male_18_24 - ((demo_male_18_29 + demo_male_30_39 + demo_male_40_49 +demo_male_50_59 +demo_male_60_69 +demo_male_70_999) - demo_male_21_plus) as demo_male_21_24,
  demo_male_18_24 as demo_male_18_24,
  (demo_male_18_29 - demo_male_18_24) as demo_male_25_29,
  demo_male_25_34 - (demo_male_18_29 - demo_male_18_24) as demo_male_30_34,
  demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)) as demo_male_35_39,
  demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24))) as demo_male_40_44,
  demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))) as demo_male_45_49,
  demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))))  as demo_male_50_54,
  demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))))) as demo_male_55_59,
  demo_male_55_64 - (demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24))))))) as demo_male_60_64,
  demo_male_60_69 - (demo_male_55_64 - (demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))))))) as demo_male_65_69,
  demo_male_65_999 - (demo_male_60_69 - (demo_male_55_64 - (demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24))))))))) as demo_male_70_999_cal,
 demo_male_25_34,  demo_male_35_44, demo_male_45_54, demo_male_55_64,  demo_male_65_999, demo_male_18_29, demo_male_30_39, demo_male_40_49, demo_male_50_59, demo_male_60_69,  demo_male_70_999,

 (demo_female_18_29 + demo_female_30_39 + demo_female_40_49 +demo_female_50_59 +demo_female_60_69 +demo_female_70_999) - demo_female_21_plus as demo_female_18_20,
  demo_female_18_24 - ((demo_female_18_29 + demo_female_30_39 + demo_female_40_49 +demo_female_50_59 +demo_female_60_69 +demo_female_70_999) - demo_female_21_plus) as demo_female_21_24,
 demo_female_18_24 as demo_female_18_24,
 (demo_female_18_29 - demo_female_18_24) as demo_female_25_29,
 demo_female_25_34 - (demo_female_18_29 - demo_female_18_24) as demo_female_30_34,
 demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)) as demo_female_35_39,
 demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24))) as demo_female_40_44,
 demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))) as demo_female_45_49,
 demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))))  as demo_female_50_54,
 demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))))) as demo_female_55_59,
 demo_female_55_64 - (demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24))))))) as demo_female_60_64,
 demo_female_60_69 - (demo_female_55_64 - (demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))))))) as demo_female_65_69,
 demo_female_65_999 - (demo_female_60_69 - (demo_female_55_64 - (demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24))))))))) as demo_female_70_999_cal,
demo_female_25_34,  demo_female_35_44, demo_female_45_54, demo_female_55_64,  demo_female_65_999, demo_female_18_29, demo_female_30_39, demo_female_40_49, demo_female_50_59, demo_female_60_69,  demo_female_70_999
from gunjanmohan.agedemo_bucket)


-- Check if the sum of the old buckets and new buckets are the same for Males 
select sum(demo_male_cal), sum(demo_male_1), sum(demo_male_2),
       sum(demo_male_18_29_cal), sum(demo_male_18_29), sum(demo_male_25_34_cal), sum (demo_male_25_34),
       sum(demo_male_35_44_cal), sum(demo_male_35_44), sum(demo_male_45_54_cal), sum(demo_male_45_54), sum(demo_male_55_64_cal),
       sum(demo_male_55_64), sum(demo_male_65_999_cal), sum(demo_male_65_999),
       sum(demo_male_30_39_cal), sum(demo_male_30_39), sum(demo_male_40_49_cal), sum(demo_male_40_49),
       sum(demo_male_50_59_cal), sum(demo_male_50_59), sum(demo_male_60_69_cal), sum(demo_male_60_69), sum(demo_male_70_999_cal), sum(demo_male_70_999)
           from (
select (demo_male_18_20 + demo_male_21_24 + demo_male_25_29 + demo_male_30_34 + demo_male_35_39 + demo_male_40_44 +
        demo_male_45_49 + demo_male_50_54 +  demo_male_55_59 + demo_male_60_64 + demo_male_65_69 + demo_male_70_999_cal) as demo_male_cal,
       (demo_male_18_24 +  demo_male_25_34 + demo_male_35_44 +demo_male_45_54 +demo_male_55_64 + demo_male_65_999)  as demo_male_1,
       (demo_male_18_29 + demo_male_30_39 + demo_male_40_49 +demo_male_50_59 +demo_male_60_69 +demo_male_70_999) as demo_male_2,
       (demo_male_18_20 + demo_male_21_24 + demo_male_25_29) as demo_male_18_29_cal,
       demo_male_18_29,
    (demo_male_25_29 + demo_male_30_34) as demo_male_25_34_cal,
    demo_male_25_34,
    (demo_male_35_39 + demo_male_40_44) as demo_male_35_44_cal,
     demo_male_35_44 ,
     (demo_male_45_49 + demo_male_50_54) as demo_male_45_54_cal,
     demo_male_45_54,
     (demo_male_55_59 + demo_male_60_64) as demo_male_55_64_cal,
     demo_male_55_64,
     (demo_male_65_69 + demo_male_70_999_cal) as demo_male_65_999_cal,
     demo_male_65_999,
     (demo_male_30_34 + demo_male_35_39) as demo_male_30_39_cal,
     demo_male_30_39,
     (demo_male_40_44 + demo_male_45_49 ) as demo_male_40_49_cal,
     demo_male_40_49,
     (demo_male_50_54 +  demo_male_55_59 ) as demo_male_50_59_cal,
     demo_male_50_59,
     (demo_male_60_64 + demo_male_65_69) as demo_male_60_69_cal,
     demo_male_60_69,
      demo_male_70_999_cal,
      demo_male_70_999
    from gunjanmohan.agedemo_bucket_new) a

