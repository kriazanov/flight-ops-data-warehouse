drop view dwh_presentation."UAE_LA_Е_RA";

create or replace view dwh_presentation."UAE_LA_Е_RA" as
select
   station,
   vendor,
   name,
   name_1,
   name_2,
   zip_code,
   city,
   state,
   country,
   department_utvu as "ОТДЕЛЕНИЕ ПО ЮТВУ",
   department_uti as "ОТДЕЛЕНИЕ ПО ЮТИ (ЛСТО)",
   longitude,
   latitude
from dwh_presentation.report_helicopters_station;

alter view dwh_presentation."UAE_LA_Е_RA" owner to dwh;
grant select on dwh_presentation."UAE_LA_Е_RA" to readonly;
