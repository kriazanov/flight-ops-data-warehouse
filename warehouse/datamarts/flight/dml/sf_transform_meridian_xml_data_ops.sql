create or replace function dwh_staging.sf_transform_meridian_xml_data_ops()
returns void
language plpgsql
as $$
declare
    c_raw_file_ids cursor for
        select id
        from dwh_staging.raw_meridian_board_xml
        where date_processed is null
        order by id
        limit 100;
begin
    for c_row in c_raw_file_ids loop

        truncate table dwh_staging.fact_leg_ops;

        delete from dwh_presentation.fact_leg
        where actual_date is null;

        insert into dwh_staging.fact_leg_ops
        select
            --id's
            md5(nullif(regexp_replace(coalesce(flight_no, ''), '\D', '', 'g'), '')::varchar || coalesce(tail_no, '')::varchar || coalesce(to_char(to_timestamp(nullif(trim(flight_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '') || coalesce(to_char(to_timestamp(nullif(trim(actual_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '')) as md5_flight,
            md5(nullif(regexp_replace(coalesce(flight_no, ''), '\D', '', 'g'), '')::varchar || coalesce(bort, '')::varchar || coalesce(to_char(to_timestamp(nullif(trim(departure_plan_utc), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '')) as md5_leg,
            dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(departure_plan_utc, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int) as id_date,
            dwh_staging.sf_dim_time(to_char(to_timestamp(nullif(departure_plan_utc, ' '), 'DD.MM.YYYY HH24:MI'), 'HH24MI')::int) as id_time,
            dwh_staging.sf_dim_flight(carrier, null, nullif(regexp_replace(flight_no, '\D', '', 'g'), ''), nullif(regexp_replace(flight_no, '[\d-]', '', 'g'), ''), cshare, null, null, null, null, null, status, stc) as id_flight,
            dwh_staging.sf_dim_board(true, bort, type_vs, name_typ) as id_board,
            dwh_staging.sf_dim_station(true, origin_iata, origin_iata, origin_icao, origin_name, origin_name_tr) as id_origin,
            dwh_staging.sf_dim_station(true, destination_iata, destination_iata, destination_icao, destination_name, destination_name_tr) as id_destination,

            --source
            'OPS' as source_code,
            'board.xml от ' || to_char(date_created, 'DD.MM.YYYY HH24:MI') as source,

            --flight
            nullif(carrier, '') as carrier_code,
            cast(nullif(flight_id, '') as integer) as external_flight_id,
            nullif(regexp_replace(flight_no, '\D', '', 'g'), '') as flight_no,
            to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY HH24:MI') as flight_date,
            nullif(regexp_replace(flight_no, '[\d-]', '', 'g'), '') as litera,
            nullif(cshare, '') as cshare,
            to_timestamp(nullif(actual_date, ' '), 'DD.MM.YYYY HH24:MI') as actual_date,

            --leg
            nullif(bort, '') as leg_tail_no,
            nullif(type_vs, '') as ac_type_code,
            nullif(name_typ, '') as ac_type_name,
            nullif(origin_iata, '') as origin_iata,
            nullif(origin_icao, '') as origin_icao,
            nullif(origin_name, '') as origin_name_ru,
            nullif(origin_name_tr, '') as origin_name_en,
            nullif(destination_iata, '') as destination_iata,
            nullif(destination_icao, '') as destination_icao,
            nullif(destination_name, '') as destination_name_ru,
            nullif(destination_name_tr, '') as destination_name_en,
            nullif(delay_codes, '') as delay_codes,
            nullif(status, '') as status,
            nullif(stc, '') as stc,
            to_timestamp(nullif(departure_plan_local, ' '), 'DD.MM.YYYY HH24:MI') as departure_plan_local,
            to_timestamp(nullif(departure_plan_utc, ' '), 'DD.MM.YYYY HH24:MI') as departure_plan_utc,
            to_timestamp(nullif(arrival_plan_local, ' '), 'DD.MM.YYYY HH24:MI') as arrival_plan_local,
            to_timestamp(nullif(arrival_plan_utc, ' '), 'DD.MM.YYYY HH24:MI') as arrival_plan_utc,
            to_timestamp(nullif(departure_est_local, ' '), 'DD.MM.YYYY HH24:MI') as departure_est_local,
            to_timestamp(nullif(departure_est_utc, ' '), 'DD.MM.YYYY HH24:MI') as departure_est_utc,
            to_timestamp(nullif(arrival_est_local, ' '), 'DD.MM.YYYY HH24:MI') as arrival_est_local,
            to_timestamp(nullif(arrival_est_utc, ' '), 'DD.MM.YYYY HH24:MI') as arrival_est_utc,
            to_timestamp(nullif(departure_fact_local, ' '), 'DD.MM.YYYY HH24:MI') as departure_fact_local,
            to_timestamp(nullif(departure_fact_utc, ' '), 'DD.MM.YYYY HH24:MI') as departure_fact_utc,
            to_timestamp(nullif(arrival_fact_local, ' '), 'DD.MM.YYYY HH24:MI') as arrival_fact_local,
            to_timestamp(nullif(arrival_fact_utc, ' '), 'DD.MM.YYYY HH24:MI') as arrival_fact_utc,
            coalesce(pax_transit, 0) as pax_transit,
            coalesce(f_nr, 0) as f_nr,
            coalesce(c_nr, 0) as c_nr,
            coalesce(y_nr, 0) as y_nr,
            coalesce(f_seats, 0) as f_seats_total,
            coalesce(c_seats, 0) as c_seats_total,
            coalesce(y_seats, 0) as y_seats_total
        from dwh_staging.raw_meridian_board_xml,
            xmltable('//FLIGHT_TYPE/FLIGHT/LEG' passing file_content columns
                --flight
                carrier varchar path '../CARRIER',
                flight_id varchar path '../FLIGHT_ID',
                flight_no varchar path '../FLIGHT_NO',
                flight_date varchar path '../FLIGHT_DATE',
                litera varchar path '../LITERA',
                cshare varchar path '../CSHARE',
                actual_date varchar path '../ACTUAL_DATE',
                tail_no varchar path '../TAIL_NO',
                --leg
                origin_name varchar path 'ORIGIN_NAME',
                origin_name_tr varchar path 'ORIGIN_NAME_TR',
                origin_icao varchar path 'ORIGIN_ICAO',
                origin_iata varchar path 'ORIGIN_IATA',
                destination_name varchar path 'DESTINATION_NAME',
                destination_name_tr varchar path 'DESTINATION_NAME_TR',
                destination_icao varchar path 'DESTINATION_ICAO',
                destination_iata varchar path 'DESTINATION_IATA',
                type_vs varchar path 'TYPE_VS',
                name_typ varchar path 'NAME_TYP',
                bort varchar path 'BORT',
                delay_codes varchar path 'DELAY_CODES',
                departure_plan_local varchar path 'DEPARTURE_PLAN_LOCAL',
                departure_plan_utc varchar path 'DEPARTURE_PLAN_UTC',
                arrival_plan_local varchar path 'ARRIVAL_PLAN_LOCAL',
                arrival_plan_utc varchar path 'ARRIVAL_PLAN_UTC',
                departure_est_local varchar path 'DEPARTURE_EST_LOCAL',
                departure_est_utc varchar path 'DEPARTURE_EST_UTC',
                arrival_est_local varchar path 'ARRIVAL_EST_LOCAL',
                arrival_est_utc varchar path 'ARRIVAL_EST_UTC',
                departure_fact_local varchar path 'DEPARTURE_FACT_LOCAL',
                departure_fact_utc varchar path 'DEPARTURE_FACT_UTC',
                arrival_fact_local varchar path 'ARRIVAL_FACT_LOCAL',
                arrival_fact_utc varchar path 'ARRIVAL_FACT_UTC',
                f_seats int path 'F_SEATS',
                c_seats int path 'C_SEATS',
                y_seats int path 'Y_SEATS',
                f_nr int path 'F_NR',
                c_nr int path 'C_NR',
                y_nr int path 'Y_NR',
                pax_transit int path 'PAX_TRANSIT',
                status varchar path 'STATUS',
                stc varchar path 'STC')
        where id = c_row.id;

        delete from dwh_presentation.fact_leg
        where source_code = 'OPS'
        and md5_leg in (select md5_leg from dwh_staging.fact_leg_ops);

        insert into dwh_presentation.fact_leg(
            md5_flight, md5_leg, id_date, id_time, id_flight, id_board, id_origin, id_destination,
            source_code, source, flight_date, actual_date, delay_codes,
            departure_plan_local, departure_plan_utc, arrival_plan_local, arrival_plan_utc,
            departure_est_local, departure_est_utc, arrival_est_local, arrival_est_utc,
            departure_fact_local, departure_fact_utc, arrival_fact_local, arrival_fact_utc,
            pax_transit, f_nr, c_nr, y_nr, f_seats_total, c_seats_total, y_seats_total, external_flight_id)
        select distinct
            md5_flight, md5_leg, id_date, id_time, id_flight, id_board, id_origin, id_destination,
            source_code, source, flight_date, actual_date, delay_codes,
            departure_plan_local, departure_plan_utc, arrival_plan_local, arrival_plan_utc,
            departure_est_local, departure_est_utc, arrival_est_local, arrival_est_utc,
            departure_fact_local, departure_fact_utc, arrival_fact_local, arrival_fact_utc,
            pax_transit, f_nr, c_nr, y_nr, f_seats_total, c_seats_total, y_seats_total,
            external_flight_id::int
        from dwh_staging.fact_leg_ops
        where source_code = 'OPS';

        update dwh_staging.raw_meridian_board_xml set date_processed = now() where id = c_row.id;

        raise notice 'OPS raw file processing with id = % complete', c_row.id;

    end loop;
end$$;