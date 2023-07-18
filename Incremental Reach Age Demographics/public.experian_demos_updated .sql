
--Creating the new table for reach incremental dashboard
DROP TABLE IF EXISTS public.experian_demos_updated ;
create table public.experian_demos_updated as (
with experian as
         (select *
          from (
                   select token, ed.*, row_number() over (partition by token order by joined_date desc) rnum
                   from detection.experian_demography_curr  ed
                           join detection.tv
                                on tv.tvid = ed.tvid
                    where  (income_0_35_hh + income_35_45_hh + income_45_55_hh + income_55_70_hh + income_70_85_hh
                                +income_85_100_hh+income_100_125_hh+income_125_150_hh+income_150_200_hh+income_200_plus_hh) = 1
               )
          where rnum = 1
         )
SELECT token, cnt,
               demo as demo_raw,
               case when demo like 'demo_%' then 'Age & Gender'
                   when demo like 'income_%' then 'Income Level'
                   when demo like 'edu_%' then 'Education'
                   when demo like 'ethnicity_%' then 'Ethnicity'
                   --when demo like 'marital_%' then  'Marital Status'
                   --when demo like 'adult_hh_size' then  'Household Size'
                   when demo in ('home_owner_hh','home_renter_hh','language_spanish','babies_0_3_hh','children_0_18_hh') then 'Other'
                    end as demo_type,
               case when demo like 'demo_female_%'
                  then replace(replace(replace(demo, 'demo_female_', 'Females '),'_999','+'),'_','-')
                when demo like 'demo_male_%'
                  then replace(replace(replace(demo, 'demo_male_', 'Males '),'_999','+'),'_','-')
                when demo like 'edu_%'
                  then initcap(replace(split_part(demo,'edu_',2),'_',' '))
                when demo like 'ethnicity_%'
                  then initcap(replace(split_part(demo,'ethnicity_',2),'_',' '))
               -- when demo like 'marital_%'
                 -- then initcap(split_part(demo,'marital_status_',2))
                when demo like 'income_%'
                  then replace(replace(replace(replace(replace(demo, 'income_', 'HH Incomes $'),'_hh',''),'plus','+'),'_','K-')+'K','-+K','+')
                when demo = 'home_owner_hh' then 'Home Owners'
                when demo = 'home_renter_hh' then 'Home Renters'
                when demo = 'babies_0_3_hh' then 'HH with Babies Age 0-3'
                when demo = 'language_spanish' then 'Spanish Speaking HH'
                when demo = 'children_0_18_hh' then 'HH with Children Age 0-18'
                --when demo = 'adult_hh_size' then 'HH Size'
                end as demo
                from (
SELECT token,
  (demo_male_18_29 + demo_male_30_39 + demo_male_40_49 +demo_male_50_59 +demo_male_60_69 +demo_male_70_999) - demo_male_21_plus as demo_male_18_20,
  demo_male_18_24 - ((demo_male_18_29 + demo_male_30_39 + demo_male_40_49 +demo_male_50_59 +demo_male_60_69 +demo_male_70_999) - demo_male_21_plus) as demo_male_21_24,
  (demo_male_18_29 - demo_male_18_24) as demo_male_25_29,
  demo_male_25_34 - (demo_male_18_29 - demo_male_18_24) as demo_male_30_34,
  demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)) as demo_male_35_39,
  demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24))) as demo_male_40_44,
  demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))) as demo_male_45_49,
  demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))))  as demo_male_50_54,
  demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))))) as demo_male_55_59,
  demo_male_55_64 - (demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24))))))) as demo_male_60_64,
  demo_male_60_69 - (demo_male_55_64 - (demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24)))))))) as demo_male_65_69,
  demo_male_65_999 - (demo_male_60_69 - (demo_male_55_64 - (demo_male_50_59- (demo_male_45_54 -   (demo_male_40_49 -   (demo_male_35_44 -   (demo_male_30_39 -   (demo_male_25_34 - (demo_male_18_29 - demo_male_18_24))))))))) as demo_male_70_999,
 (demo_female_18_29 + demo_female_30_39 + demo_female_40_49 +demo_female_50_59 +demo_female_60_69 +demo_female_70_999) - demo_female_21_plus as demo_female_18_20,
  demo_female_18_24 - ((demo_female_18_29 + demo_female_30_39 + demo_female_40_49 +demo_female_50_59 +demo_female_60_69 +demo_female_70_999) - demo_female_21_plus) as demo_female_21_24,
 (demo_female_18_29 - demo_female_18_24) as demo_female_25_29,
 demo_female_25_34 - (demo_female_18_29 - demo_female_18_24) as demo_female_30_34,
 demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)) as demo_female_35_39,
 demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24))) as demo_female_40_44,
 demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))) as demo_female_45_49,
 demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))))  as demo_female_50_54,
 demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))))) as demo_female_55_59,
 demo_female_55_64 - (demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24))))))) as demo_female_60_64,
 demo_female_60_69 - (demo_female_55_64 - (demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24)))))))) as demo_female_65_69,
 demo_female_65_999 - (demo_female_60_69 - (demo_female_55_64 - (demo_female_50_59- (demo_female_45_54 -   (demo_female_40_49 -   (demo_female_35_44 -   (demo_female_30_39 -   (demo_female_25_34 - (demo_female_18_29 - demo_female_18_24))))))))) as demo_female_70_999,
            edu_college,
            edu_graduate,
            edu_high_school,
            edu_some_college,
            ethnicity_african_american,
            ethnicity_asian,
            ethnicity_white_non_hispanic,
            ethnicity_hispanic,
            ethnicity_middle_eastern,
            ethnicity_native_american,
            language_spanish,
            marital_status_married,
            marital_status_single,
            home_owner_hh,
            home_renter_hh,
            income_0_35_hh,
            income_35_45_hh,
            income_45_55_hh,
            income_55_70_hh,
            income_70_85_hh,
            income_85_100_hh,
            income_100_125_hh,
            income_125_150_hh,
            income_150_200_hh,
            income_200_plus_hh,
            babies_0_3_hh,
            children_0_18_hh,
            move_likely_hh,
            move_recent_hh,
            adult_hh_size,
            low_quality,
            demo_incomplete
        FROM experian)
        UNPIVOT (
        cnt FOR demo IN  (
                            demo_male_18_20,
                            demo_male_21_24,
                            demo_male_25_29,
                            demo_male_30_34,
                            demo_male_35_39,
                            demo_male_40_44,
                            demo_male_45_49,
                            demo_male_50_54,
                            demo_male_55_59,
                            demo_male_60_64,
                            demo_male_65_69,
                            demo_male_70_999,
                            demo_female_18_20,
                            demo_female_21_24,
                            demo_female_25_29,
                            demo_female_30_34,
                            demo_female_35_39,
                            demo_female_40_44,
                            demo_female_45_49,
                            demo_female_50_54,
                            demo_female_55_59,
                            demo_female_60_64,
                            demo_female_65_69,
                            demo_female_70_999,
                            edu_college,
                            edu_graduate,
                            edu_high_school,
                            edu_some_college,
                            ethnicity_african_american,
                            ethnicity_asian,
                            ethnicity_white_non_hispanic,
                            ethnicity_hispanic,
                            ethnicity_middle_eastern,
                            ethnicity_native_american,
                            language_spanish,
                            marital_status_married,
                            marital_status_single,
                            home_owner_hh,
                              home_renter_hh,
                              income_0_35_hh,
                              income_35_45_hh,
                              income_45_55_hh,
                              income_55_70_hh,
                              income_70_85_hh,
                              income_85_100_hh,
                              income_100_125_hh,
                              income_125_150_hh,
                              income_150_200_hh,
                              income_200_plus_hh,
                              babies_0_3_hh,
                              children_0_18_hh,
                              move_likely_hh,
                              move_recent_hh,
                              adult_hh_size,
                              low_quality,
                              demo_incomplete)
                                  )
WHERE cnt > 0 )