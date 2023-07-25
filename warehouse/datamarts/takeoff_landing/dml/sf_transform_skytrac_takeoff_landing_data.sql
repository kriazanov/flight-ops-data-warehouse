create or replace function dwh_staging.sf_transform_skytrac_takeoff_landing_data()
returns void
language plpgsql
as $$
begin
    -- preparations
    truncate table dwh_staging.fact_takeoff_landing;

    -- decoding raw data
    insert into dwh_staging.fact_takeoff_landing(id_source, id_fact_leg, id_takeoff_landing_kind, id_date, id_time, id_board, id_flight, id_airport, id_captain, id_runway, datetime, skytrac_approx_position, track, latitude, longitude, altitude_ft, speed_kts, distance_m)
    select
        rsm.id as id_source,
        null as id_fact_leg,
        dj.id as id_takeoff_landing_kind,
        dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(replace(rsm.decoded_iso_datetime_utc, 'T', ' '), ' '), 'YYYY-MM-DD HH24:MI:SS'), 'YYYYMMDD')::int) as id_date,
        dwh_staging.sf_dim_time(to_char(to_timestamp(nullif(replace(rsm.decoded_iso_datetime_utc, 'T', ' '), ' '), 'YYYY-MM-DD HH24:MI:SS'), 'HH24MI')::int) as id_time,
        dwh_staging.sf_dim_board(false, rsm.raw_board, null, null) as id_board,
        null as id_flight,
        null as id_airport,
        null as id_captain,
        null as id_runway,

        decoded_iso_datetime_utc::timestamp as datetime,
        raw_skytrac_approx_position as skytrac_approx_position,
        replace(raw_track, ' T', '')::numeric as track,
        nullif(decoded_lat_new_format, '')::numeric as latitude,
        nullif(decoded_long_new_format, '')::numeric as longitude,
        nullif(decoded_alt_value, '')::numeric as altitude_ft,
        nullif(decoded_speed_value, '')::numeric as speed_kts,
        null as distance_m

    from dwh_staging.raw_skytrac_mails rsm
    left join dwh_presentation.dim_junk dj on dj.takeoff_landing_kind = rsm.raw_takeoff_landing_kind

    where rsm.date_processed is null
    and decoded_speed_value != 'Track:';

    -- updating id's
    update dwh_staging.fact_takeoff_landing
    set id_runway = dwh_staging.sf_dim_runway(coalesce(track, 0), latitude, longitude)
    where 1=1;

    update dwh_staging.fact_takeoff_landing ftl
    set id_airport = ds.id
    from dwh_presentation.dim_runway dr
    left join dwh_presentation.dim_station ds on ds.station_iata = dr.airport_iata
    where ftl.id_runway = dr.id;

    update dwh_staging.fact_takeoff_landing ftl
    set id_fact_leg = (
        select fl.id
        from dwh_presentation.fact_leg fl
        where fl.id_board = ftl.id_board
        and case ftl.id_takeoff_landing_kind
                when 1 /*takeoff*/ then fl.id_origin
                when 2 /*landing*/ then fl.id_destination
            end = ftl.id_airport
        order by
            case ftl.id_takeoff_landing_kind
                when 1 /*takeoff*/ then abs(extract(epoch from ftl.datetime - departure_fact_utc))
                when 2 /*landing*/ then abs(extract(epoch from ftl.datetime - arrival_fact_utc))
            end
        limit 1)
    where ftl.id_fact_leg is null;

    update dwh_staging.fact_takeoff_landing ftl
    set id_flight = fl.id_flight, id_captain = dctm.id
    from dwh_presentation.fact_leg fl
    left join dwh_presentation.fact_crewtask fct on fct.md5_flight = fl.md5_flight
    left join dwh_presentation.dim_crew_task_member dctm on dctm.id = fct.id_crew_task_member
    where fl.id = ftl.id_fact_leg
    and dctm.role = 'КВС'
    and id_captain is null
    and id_fact_leg is not null;

    -- calculating facts
    update dwh_staging.fact_takeoff_landing u
    set distance_m = st_distancesphere(st_point(f.longitude, f.latitude), st_point(d.runway_end_long, d.runway_end_lat))
    from dwh_staging.fact_takeoff_landing f
    inner join dwh_presentation.dim_runway d on d.id = f.id_runway
    where u.id = f.id
    and u.id_fact_leg is not null;

end$$;