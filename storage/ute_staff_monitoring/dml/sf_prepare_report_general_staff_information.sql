create or replace function dwh_staging.sf_prepare_report_general_staff_information()
    returns void
    language plpgsql
as
$$
begin

    truncate dwh_presentation.report_general_staff_information;

    insert into dwh_presentation.report_general_staff_information(
        login, first_last_name, birthday, sex, otdel, class, rating_date, work_place_number, work_place, zup_empl_guid,
        personnel_number, zfo, job_title, specialization, start_work_date, end_work_date, work_status, work_place2,
        location, subdivision, work_shift, certificate_number, total_work_experience, apk_validity_date,
        certificate_give_date, validity_authorisation_date, status)
    select
        login,
        first_last_name,
        to_date(birthday, 'dd.mm.yyyy'),
        sex,
        otdel,
        class,
        rating_date,
        work_place_number,
        work_place,
        zup_empl_guid,
        personnel_number,
        zfo,
        job_title,
        specialization,
        to_date(start_work_date, 'DD.MM.YYYY'),
        to_date(end_work_date, 'DD.MM.YYYY'),
        work_status,
        work_place2,
        location,
        subdivision,
        work_shift,
        certificate_number,
        total_work_experience,
        to_date(apk_validity_date, 'DD.MM.YYYY'),
        certificate_give_date,
        validity_authorisation_date,
        status
    from dwh_staging.report_general_staff_information;

end
$$;