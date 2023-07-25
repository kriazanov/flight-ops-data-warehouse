do $dwh_staging$
begin
    drop table dwh_staging.report_heli_status_repair;

    create table dwh_staging.report_heli_status_repair
    (
        ac_registr varchar,
        owner_name varchar,
        acceptance_in_percent varchar,
        disassembly_in_percent varchar,
        repair_in_percent varchar,
        painting_in_percent varchar,
        assembly_in_percent varchar,
        testing_in_percent varchar,
        passing_in_percent varchar,
        plan_time_in_percent varchar,
        description varchar,
        date_repair date,
        in_apk integer,
        apk_start_date_int integer,
        vfdates date,
        edate date,
        in_delay integer,
        last_flight date,
        first_flight date,
        apk_start_date date,
        apk_end_date date,
        col varchar,
        cm_fact integer,
        cm_plan integer,
        disassembly_time integer,
        disassembly_duration integer,
        assembly_time integer,
        assembly_duration integer,
        wpno varchar);

    alter table dwh_staging.report_heli_status_repair owner to dwh;
    grant select on dwh_staging.report_heli_status_repair to readonly;
end;
$dwh_staging$;

do $dwh_presentation$
begin
drop table dwh_presentation.report_heli_status_repair;
create table dwh_presentation.report_heli_status_repair(
    ac_registr varchar,
    owner_name varchar,
    acceptance_in_percent varchar,
    disassembly_in_percent varchar,
    repair_in_percent varchar,
    painting_in_percent varchar,
    assembly_in_percent varchar,
    testing_in_percent varchar,
    passing_in_percent varchar,
    plan_time_in_percent varchar,
    description varchar,
    date_repair date,
    in_apk integer,
    apk_start_date_int integer,
    vfdates date,
    edate date,
    in_delay integer,
    last_flight date,
    first_flight date,
    apk_start_date date,
    apk_end_date date,
    col varchar,
    cm_fact integer,
    cm_plan integer,
    disassembly_time integer,
    disassembly_duration integer,
    assembly_time integer,
    assembly_duration integer,
    wpno varchar);

    alter table dwh_presentation.report_heli_status_repair owner to dwh;
    grant select on dwh_presentation.report_heli_status_repair to readonly;
end;
$dwh_presentation$;

