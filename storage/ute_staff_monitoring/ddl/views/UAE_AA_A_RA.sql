drop view dwh_presentation."UAE_AA_A_RA";

create view dwh_presentation."UAE_AA_A_RA" as
select *
from dwh_presentation.report_salary_piecework;

alter view dwh_presentation."UAE_AA_A_RA" owner to dwh;
grant select on dwh_presentation."UAE_AA_A_RA" to readonly;
