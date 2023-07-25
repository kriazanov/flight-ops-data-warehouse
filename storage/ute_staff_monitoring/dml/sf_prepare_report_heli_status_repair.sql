create or replace function dwh_staging.sf_prepare_report_heli_status_repair() returns void
    language plpgsql
as
$$
begin
    truncate table dwh_presentation.report_heli_status_repair;

    insert into dwh_presentation.report_heli_status_repair
    select * from dwh_staging.report_heli_status_repair;
end
$$;

alter function dwh_staging.sf_prepare_report_heli_status_repair() owner to dwh;