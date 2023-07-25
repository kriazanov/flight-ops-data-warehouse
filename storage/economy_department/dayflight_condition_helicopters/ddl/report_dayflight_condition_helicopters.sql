do $dwh_staging$
begin
    drop table dwh_staging.report_dayflight_condition_helicopters;
    create table dwh_staging.report_dayflight_condition_helicopters(
        id serial,
        ac_type varchar,
        ac_registr varchar,
        dislocation varchar,
        base varchar,
        base_date varchar,
        marker varchar,
        color varchar,
        flight_minutes numeric
    );
    alter table dwh_staging.report_dayflight_condition_helicopters owner to dwh;
    grant select on dwh_staging.report_dayflight_condition_helicopters to readonly;
end;
$dwh_staging$;

do $dwh_presentation$
begin
    drop table dwh_presentation.report_dayflight_condition_helicopters;
    create table dwh_presentation.report_dayflight_condition_helicopters(
        id serial,
        ac_type varchar,
        ac_registr varchar,
        dislocation varchar,
        base varchar,
        base_date varchar,
        marker varchar,
        color varchar,
        flight_minutes numeric
    );
    alter table dwh_presentation.report_dayflight_condition_helicopters owner to dwh;
    grant select on dwh_presentation.report_dayflight_condition_helicopters to readonly;
end;
$dwh_presentation$;

