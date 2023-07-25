create or replace function dwh_staging.sf_dim_station(
    p_is_update boolean,
	p_code varchar,
	p_station_iata varchar,
	p_station_icao varchar,
	p_station_name_ru varchar,
	p_station_name_en varchar)
returns integer
language plpgsql
as $$
declare
    v_id integer;
begin
    if p_is_update = true
    then
        insert into dwh_presentation.dim_station(code, station_iata, station_icao, station_name_ru, station_name_en)
        select
            p_code as code,
            p_station_iata as station_iata,
            p_station_icao as station_icao,
            p_station_name_ru as station_name_ru,
            p_station_name_en as station_name_en
        on conflict(code) do update
        set code = excluded.station_iata,
            station_iata = excluded.station_iata,
            station_icao = excluded.station_icao,
            station_name_ru = excluded.station_name_ru,
            station_name_en = excluded.station_name_en
        returning id into v_id;
    else
        insert into dwh_presentation.dim_station(code, station_iata, station_icao, station_name_ru, station_name_en)
        select
            p_code as code,
            p_station_iata as station_iata,
            p_station_icao as station_icao,
            p_station_name_ru as station_name_ru,
            p_station_name_en as station_name_en
        on conflict(code) do nothing;

        select id into v_id
        from dwh_presentation.dim_station
        where 1=1
        and code = p_code;
    end if;

    return v_id;
end;
$$;