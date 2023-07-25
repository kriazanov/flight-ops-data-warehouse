create or replace function dwh_staging.sf_prepare_report_helicopters_station()
returns void
language plpgsql
as $$
begin
    truncate dwh_presentation.report_helicopters_station;

    insert into dwh_presentation.report_helicopters_station
    select * from dwh_staging.report_helicopters_station;

end$$;