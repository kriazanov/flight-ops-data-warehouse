do $dwh_staging$
begin
    create table dwh_staging.report_helicopters_station(
        station varchar,
        vendor varchar,
        name varchar,
        name_1 varchar,
        name_2 varchar,
        zip_code varchar,
        city varchar,
        state varchar,
        country varchar,
        department_utvu varchar,
        department_uti varchar,
        longitude float,
        latitude float
    );

    alter table dwh_staging.report_helicopters_station owner to dwh;
end;
$dwh_staging$;

do $dwh_presentation$
begin
    create table dwh_presentation.report_helicopters_station(
        station varchar,
        vendor varchar,
        name varchar,
        name_1 varchar,
        name_2 varchar,
        zip_code varchar,
        city varchar,
        state varchar,
        country varchar,
        department_utvu varchar,
        department_uti varchar,
        longitude float,
        latitude float
    );

    alter table dwh_presentation.report_helicopters_station owner to dwh;
end;
$dwh_presentation$;