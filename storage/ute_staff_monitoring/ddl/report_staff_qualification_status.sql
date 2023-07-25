do
$dwh_staging$
begin
    drop table dwh_staging.report_staff_qualification_status;
    create table dwh_staging.report_staff_qualification_status(
        login varchar,
        first_last_name varchar,
        personnel_number varchar,
        type_label varchar,
        issue_date varchar,
        ac_type varchar,
        apk_description varchar,
        ac_model varchar,
        ac_sub_type varchar,
        skill varchar,
        scope varchar,
        expiry_date varchar,
        passed varchar);

    alter table dwh_staging.report_staff_qualification_status owner to dwh;
end;
$dwh_staging$;

do
$dwh_presentation$
begin
    drop table dwh_presentation.report_staff_qualification_status;
    create table dwh_presentation.report_staff_qualification_status(
        login varchar,
        first_last_name varchar,
        personnel_number varchar,
        type_label varchar,
        issue_date date,
        ac_type varchar,
        apk_description varchar,
        ac_model varchar,
        ac_sub_type varchar,
        skill varchar,
        scope varchar,
        expiry_date date,
        passed varchar);

    alter table dwh_presentation.report_staff_qualification_status owner to dwh;
end;
$dwh_presentation$;