create view dwh_presentation."UAE_BA_B_RA" as
select *
from dwh_presentation.report_heli_status_repair;

alter view dwh_presentation."UAE_BA_B_RA" owner to dwh;
grant select on dwh_presentation."UAE_BA_B_RA" to readonly;