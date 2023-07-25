create view dwh_presentation."UAE_SB_J_AA" AS
select
   login as "Логин",
   first_last_name as "Имя фамилия",
   personnel_number as "Табельный номер",
   type_label as "Квалификационная отметка",
   issue_date as "Дата выпуска",
   ac_type as "ТИП ВС",
   apk_description as "Описание АРК",
   ac_model as "Модель ВС",
   ac_sub_type as "Подтип ВС",
   skill,
   scope,
   expiry_date as "СРОК ДЕЙСТВИЯ",
   passed as "приостановлено"
from dwh_presentation.report_staff_qualification_status;

alter view dwh_presentation."UAE_SB_J_AA" owner to dwh;
grant select on dwh_presentation."UAE_SB_J_AA" to readonly;