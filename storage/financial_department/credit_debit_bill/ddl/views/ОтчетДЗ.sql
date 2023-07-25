drop view dwh_presentation.ОтчетДЗ;

create view dwh_presentation.ОтчетДЗ as
select
    pfm_text as pfm_text,
    partner as partner,
    partner_name as partner_name,
    document_int_number as "Номер договора",
    document_int_ext_date as "Дата заключения договора",
    document_number_external as "Внешний № документа",
    document_date as "Дата документа",
    user_name as "Имя пользователя",
    p_stat as "ИндПрос",
    pay_stat as "Стат. задолж.",
    days_pay as "Дней до оплаты",
    pay_date as "Дата к оплате",
    sum_rub as "Сумма в рублях/Сумма ВВ",
    sum_curr as "Сумма в валюте/Сумма ВД",
    currency as "Валюта",
    pay_doc_term as "Условие из дог.",
    appropriation as "Счет ГК текст",
    number_general_book as "Счет Главной книги",
    document_number as "№ документа"
from dwh_presentation.report_debit_credit_bill
where credit_or_debit = 'D';

alter view dwh_presentation.ОтчетДЗ owner to dwh;
grant select on dwh_presentation.ОтчетДЗ to readonly;