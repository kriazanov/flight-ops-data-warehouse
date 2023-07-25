create or replace function dwh_staging.sf_dim_runway(p_track numeric, p_latitude numeric, p_longiture numeric)
returns integer
language plpgsql
as $$
declare
    v_id integer;
begin
    select dr.id
    into v_id
    from dwh_presentation.dim_runway dr
    left join dwh_presentation.dim_station ds on ds.station_iata = dr.airport_iata
    where ds.station_iata = (
        select ds.station_iata
        from dwh_presentation.dim_runway dr
        left join dwh_presentation.dim_station ds on ds.station_iata = dr.airport_iata
        order by st_distancesphere(st_point(p_longiture, p_latitude), st_point(dr.runway_end_long, dr.runway_end_lat))
        limit 1)
    order by abs(p_track - dr.runway_course_true_brg)
    limit 1;

    return v_id;
end;
$$;