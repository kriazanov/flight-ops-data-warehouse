create or replace view dwh_presentation."UAE_AA_L_RA" as
select *
from dwh_presentation.report_helicopters_fleet;

alter view dwh_presentation."UAE_AA_L_RA" owner to dwh;
grant select on dwh_presentation."UAE_AA_L_RA" to readonly;