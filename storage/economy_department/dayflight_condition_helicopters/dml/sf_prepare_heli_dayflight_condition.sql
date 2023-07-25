create or replace function dwh_staging.sf_prepare_heli_dayflight_condition()
returns void
language plpgsql
as $$
begin
    truncate table dwh_presentation.report_dayflight_condition_helicopters;

    insert into dwh_presentation.report_dayflight_condition_helicopters
    select
        c.id, dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(c.base_date, ' '), 'DD.MM.YYYY'), 'YYYYMMDD')::int) as id_date,
        c.ac_type, a.ac_type_group, c.ac_registr, c.dislocation, c.base, p.direction_code as base_direction, base_date, marker, color, flight_minutes
    from dwh_staging.report_dayflight_condition_helicopters c
    left join dwh_staging.dict_heli_dayflight_aircraft_type a on a.ac_type = c.ac_type
    left join dwh_staging.dict_heli_dayflight_point p on p.area_name = c.base;

end$$;