drop view dwh_presentation."UAE_AA_K_RA";

create view dwh_presentation."UAE_AA_K_RA" as
select *
from dwh_presentation.report_helicopters_utilization;

alter view dwh_presentation."UAE_AA_K_RA" owner to dwh;
grant select on dwh_presentation."UAE_AA_K_RA" to readonly;
