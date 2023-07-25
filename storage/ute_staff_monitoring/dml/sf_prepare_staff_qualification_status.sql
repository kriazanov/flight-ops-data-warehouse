create or replace function dwh_staging.sf_prepare_report_staff_qualification_status()
    returns void
    language plpgsql
as
$$
begin

    truncate dwh_presentation.report_staff_qualification_status;

    insert into dwh_presentation.report_staff_qualification_status(
        login, first_last_name, personnel_number, type_label, issue_date, ac_type, apk_description, ac_model,
        ac_sub_type, skill, scope, expiry_date, passed)
    select login,
           first_last_name,
           personnel_number,
           type_label,
           to_date(issue_date, 'DD.MM.YYYY'),
           ac_type,
           apk_description,
           ac_model,
           ac_sub_type,
           skill,
           scope,
           to_date(expiry_date, 'DD.MM.YYYY'),
           passed
    from dwh_staging.report_staff_qualification_status;

end
$$;