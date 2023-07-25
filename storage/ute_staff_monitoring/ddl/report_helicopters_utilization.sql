do
$dwh_staging$
begin

    drop table dwh_staging.report_helicopters_utilization;
    create table dwh_staging.report_helicopters_utilization
    (
        ac_registr varchar,
        ac_typ varchar,
        off_block_date varchar,
        departure_date varchar,
        arr_date varchar,
        on_block_date varchar,
        operator varchar,
        flight varchar,
        service_type varchar,
        flightlog_no varchar,
        per_day varchar,
        daily_hours varchar,
        daily_cycles varchar,
        block_hours varchar,
        tah varchar,
        ch varchar,
        tac varchar,
        departure varchar,
        arrival varchar,
        off_block_time_hours varchar,
        off_block_time_minutes varchar,
        departure_time_hours varchar,
        departure_time_minutes varchar,
        arrival_time_hours varchar,
        arrival_time_minutes varchar,
        on_block_time_hours varchar,
        on_block_time_minutes varchar,
        is_updated bool);
end;
$dwh_staging$;


do
$dwh_presentation$
begin
    drop table dwh_presentation.report_helicopters_utilization;
    create table dwh_presentation.report_helicopters_utilization
    (
        ac_registr varchar,
        ac_typ varchar,
        off_block_date date,
        departure_date date,
        arr_date date,
        on_block_date date,
        operator varchar,
        flight varchar,
        service_type varchar,
        flightlog_no varchar,
        per_day int,
        daily_hours float,
        daily_cycles float,
        block_hours float,
        tah int,
        ch float,
        tac int,
        departure varchar,
        arrival varchar,
        off_block_time_hours int,
        off_block_time_minutes int,
        departure_time_hours int,
        departure_time_minutes int,
        arrival_time_hours int,
        arrival_time_minutes int,
        on_block_time_hours int,
        on_block_time_minutes int);
end;
$dwh_presentation$;