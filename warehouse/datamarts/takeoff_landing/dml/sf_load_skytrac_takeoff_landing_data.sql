create or replace function dwh_staging.sf_load_skytrac_takeoff_landing_data()
returns void
language plpgsql
as $$
begin
    -- publishing to presentation layer
    insert into dwh_presentation.fact_takeoff_landing(
        id_fact_leg, id_takeoff_landing_kind, id_date, id_time, id_board, id_flight, id_airport, id_captain, id_runway,
        datetime, skytrac_approx_position, track, latitude, longitude, altitude_ft, speed_kts, distance_m)
    select
        id_fact_leg, id_takeoff_landing_kind, id_date, id_time, id_board, id_flight, id_airport, id_captain, id_runway,
        datetime, skytrac_approx_position, track, latitude, longitude, altitude_ft, speed_kts, distance_m
    from dwh_staging.fact_takeoff_landing;

    -- finalisation
    update dwh_staging.raw_skytrac_mails
    set date_processed = now()
    where date_processed is null;
end$$;