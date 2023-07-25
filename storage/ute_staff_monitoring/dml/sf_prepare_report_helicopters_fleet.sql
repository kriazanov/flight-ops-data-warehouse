create or replace function dwh_staging.sf_prepare_report_helicopters_fleet()
returns void
language plpgsql
as $$
begin
    truncate table dwh_presentation.report_helicopters_fleet;

    insert into dwh_presentation.report_helicopters_fleet
    select * from dwh_staging.report_helicopters_fleet;

end$$;