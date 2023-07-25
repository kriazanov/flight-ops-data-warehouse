create or replace function dwh_staging.sf_perform_report_helicopters_utilization()
    returns void
    language plpgsql
as
$$
begin
    insert into dwh_presentation.report_helicopters_utilization
    select 
        ac_registr,
        ac_typ,
        to_date(off_block_date, 'dd.mm.yyyy'),
        to_date(departure_date, 'dd.mm.yyyy'),
        to_date(arr_date, 'DD.MM.YYYY'),
        to_date(on_block_date, 'DD.MM.YYYY'),
        operator,
        flight,
        service_type,
        flightlog_no,
        per_day::int,
        daily_hours::float,
        daily_cycles::float,
        block_hours::float,
        tah::int,
        ch::float,
        tac::int,
        departure,
        arrival,
        off_block_time_hours::int,
        off_block_time_minutes::int,
        departure_time_hours::int,
        departure_time_minutes::int,
        arrival_time_hours::int,
        arrival_time_minutes::int,
        on_block_time_hours::int,
        on_block_time_minutes::int
    from dwh_staging.report_helicopters_utilization rhu
    where rhu.is_updated = false;

    update dwh_staging.report_helicopters_utilization
    set is_updated = true
    where 1 = 1;

    truncate table dwh_staging.report_helicopters_utilization;
end
$$;

alter function dwh_staging.sf_perform_report_helicopters_utilization() owner to dwh;