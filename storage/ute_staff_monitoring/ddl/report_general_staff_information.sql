do
$dwh_staging$
    begin
        drop table dwh_staging.report_general_staff_information;
        create table dwh_staging.report_general_staff_information
        (
            login varchar,
            first_last_name varchar,
            birthday varchar,
            sex varchar,
            otdel varchar,
            class varchar,
            rating_date varchar,
            work_place_number varchar,
            work_place varchar,
            zup_empl_guid varchar,
            personnel_number varchar,
            zfo varchar,
            job_title varchar,
            specialization varchar,
            start_work_date varchar,
            end_work_date varchar,
            work_status varchar,
            work_place2 varchar,
            location varchar,
            subdivision varchar,
            work_shift varchar,
            certificate_number varchar,
            total_work_experience varchar,
            apk_validity_date varchar,
            certificate_give_date varchar,
            validity_authorisation_date varchar,
            status varchar
        );

        alter table dwh_staging.report_general_staff_information
            owner to dwh;
    end;
$dwh_staging$;

do
$dwh_presentation$
    begin
        drop table dwh_presentation.report_general_staff_information;
        create table dwh_presentation.report_general_staff_information
        (
            login varchar,
            first_last_name varchar,
            birthday date,
            sex varchar,
            otdel varchar,
            class varchar,
            rating_date varchar,
            work_place_number varchar,
            work_place varchar,
            zup_empl_guid varchar,
            personnel_number varchar,
            zfo varchar,
            job_title varchar,
            specialization varchar,
            start_work_date date,
            end_work_date date,
            work_status varchar,
            work_place2 varchar,
            location varchar,
            subdivision varchar,
            work_shift varchar,
            certificate_number varchar,
            total_work_experience varchar,
            apk_validity_date date,
            certificate_give_date varchar,
            validity_authorisation_date varchar,
            status varchar
        );

        alter table dwh_presentation.report_general_staff_information
            owner to dwh;
    end;
$dwh_presentation$;