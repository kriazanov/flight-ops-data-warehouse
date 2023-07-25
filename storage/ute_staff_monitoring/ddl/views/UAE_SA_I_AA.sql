create view dwh_presentation."UAE_SA_I_AA" AS
select
   login as "Логин",
   first_last_name as "Имя фамилия",
   birthday as "Дата рождения",
   sex as "Пол",
   otdel as "Отдел",
   class as "Общий класс",
   rating_date as "Дата оценки",
   work_place_number as "№ Рабочего места",
   work_place as "Место работы",
   zup_empl_guid as "ZUP_EMPL_GUID",
   personnel_number as "Табельный номер",
   zfo as "ЦФО",
   job_title as "Должность",
   specialization as "Специализация",
   start_work_date as "ДАТА НАЧАЛА РАБОТЫ",
   end_work_date as "ДАТА УВОЛЬНЕНИЯ",
   work_status as "ТРУДОВОЙ СТАТУС (уволен\работает)",
   work_place2 as "Место работы2",
   location as "Локация",
   subdivision as "Подразделение",
   work_shift as "Смена",
   certificate_number as "№ Свидетельства",
   total_work_experience as "Общий стаж работы",
   apk_validity_date as "Срок действия АРК",
   certificate_give_date as "Дата выдачи свидетельства",
   validity_authorisation_date as "Срок действия авторизации",
   status
from dwh_presentation.report_general_staff_information;

alter view dwh_presentation."UAE_SA_I_AA" owner to dwh;
grant select on dwh_presentation."UAE_SA_I_AA" to readonly;