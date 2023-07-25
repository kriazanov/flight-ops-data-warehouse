create or replace function dwh_staging.sf_transform_meridian_xml_data_sal()
returns void
language plpgsql
as $$
declare
    c_raw_file_ids cursor for
        select id
        from dwh_staging.raw_meridian_sap_xml
        where date_processed is null
        order by id
        limit 100;
begin
    for c_row in c_raw_file_ids loop

        -- extracting raw data
        truncate table dwh_staging.fact_leg_sal;

        insert into dwh_staging.fact_leg_sal
        select
            -- id's
            md5(coalesce(flight_no::varchar, '') || coalesce(flight_tail_no::varchar, '') || coalesce(to_char(to_timestamp(nullif(trim(flight_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '') || coalesce(to_char(to_timestamp(nullif(trim(actual_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '')) as md5_flight,
            md5(coalesce(flight_no::varchar, '') || coalesce(leg_tail_no::varchar, '') || coalesce(to_char(to_timestamp(nullif(trim(departure_plan), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '')) as md5_leg,
            dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(departure_plan, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int) as id_date,
            dwh_staging.sf_dim_time(to_char(to_timestamp(nullif(departure_plan, ' '), 'DD.MM.YYYY HH24:MI'), 'HH24MI')::int) as id_time,
            dwh_staging.sf_dim_flight(carrier, subcarrier_code, flight_no, litera, cshare, conn_type, route, flt_kind, transp_type, remark, null, null) as id_flight,
            dwh_staging.sf_dim_board(false, leg_tail_no, null, null) as id_board,
            dwh_staging.sf_dim_station(false, origin, origin, null, null, null) as id_origin,
            dwh_staging.sf_dim_station(false, destination, destination, null, null, null) as id_destination,

            -- source
            'SAL' as source_code,
            replace(raw_meridian_sap_xml.file_name, '//10.131.10.99/Meridian_exports/asia_exp/SAP/.done/', '') as source,

            -- flight
            nullif(carrier, '') as carrier_code,
            nullif(subcarrier_code, '') as subcarrier_code,
            nullif(flight_no, '') as flight_no,
            to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY  HH24:MI') as flight_date,
            nullif(litera, '') as litera,
            nullif(cshare, '') as cshare,
            nullif(conn_type, '') as conn_type,
            nullif(flight_tail_no, '') as flight_tail_no,
            nullif(route, '') as route,
            nullif(flt_kind, '') as flt_kind,
            nullif(transp_type, '') as transp_type,
            nullif(remark, '') as remark,
            to_timestamp(nullif(actual_date, ' '), 'DD.MM.YYYY  HH24:MI') as actual_date,

            --leg
            nullif(leg_tail_no, '') as leg_tail_no,
            nullif(origin, '') as origin_iata,
            nullif(destination, '') as destination_iata,
            to_timestamp(nullif(departure_plan_local, ' '), 'DD.MM.YYYY HH24:MI')as departure_plan_local,
            to_timestamp(nullif(departure_plan, ' '), 'DD.MM.YYYY HH24:MI')as departure_plan_utc,
            to_timestamp(nullif(arrival_plan_local, ' '), 'DD.MM.YYYY HH24:MI')as arrival_plan_local,
            to_timestamp(nullif(arrival_plan, ' '), 'DD.MM.YYYY HH24:MI')as arrival_plan_utc,
            to_timestamp(nullif(departure_fact_local, ' '), 'DD.MM.YYYY HH24:MI') as departure_fact_local,
            to_timestamp(nullif(departure_fact, ' '), 'DD.MM.YYYY HH24:MI') as departure_fact_utc,
            to_timestamp(nullif(arrival_fact_local, ' '), 'DD.MM.YYYY HH24:MI') as arrival_fact_local,
            to_timestamp(nullif(arrival_fact, ' '), 'DD.MM.YYYY HH24:MI') as arrival_fact_utc,
            to_timestamp(nullif(power_on, ' '), 'DD.MM.YYYY HH24:MI') as power_on,
            to_timestamp(nullif(power_off, ' '), 'DD.MM.YYYY HH24:MI') as power_off,
            to_char(to_timestamp(nullif(on_surface, ' '), 'DD.MM.YYYY HH24:MI'), 'HH24:MI') as on_surface,
            coalesce(f_nr, 0) as f_nr,
            coalesce(c_nr, 0) as c_nr,
            coalesce(y_nr, 0) as y_nr,
            coalesce(child_nr, 0) as child_nr,
            coalesce(infant_nr, 0) as infant_nr,
            coalesce(crg_load, 0) as crg_load,
            coalesce(luggage_load, 0) as luggage_load,
            coalesce(hand_luggage, 0) as hand_luggage,
            coalesce(mail_load, 0) as mail_load,
            coalesce(cabin_baggage, 0) as cabin_baggage,
            coalesce(emptying, 0) as emptying,
            coalesce(fueling, 0) as fueling,
            coalesce(dep_remnant, 0) as dep_remnant,
            coalesce(arr_remnant, 0) as arr_remnant,
            coalesce(f_seats, 0) as f_seats,
            coalesce(c_seats, 0) as c_seats,
            coalesce(y_seats, 0) as y_seats,
            coalesce(payload, 0) as payload,
            coalesce(f_child, 0) as f_child,
            coalesce(c_child, 0) as c_child,
            coalesce(y_child, 0) as y_child,
            coalesce(f_infant, 0) as f_infant,
            coalesce(c_infant, 0) as c_infant,
            coalesce(y_infant, 0) as y_infant,
            coalesce(distance, 0) as distance,
            coalesce(equipped_empty_kg, 0) as equipped_empty_kg,
            flight_task as flight_task

        from dwh_staging.raw_meridian_sap_xml,
            xmltable('//PERFORMED_FLIGHTS/FLIGHT/LEG' passing file_content columns
                --flight
                subcarrier_code varchar path '../SUBCARRIER_CODE',
                carrier varchar path '../CARRIER',
                flight_no varchar path '../FLIGHT_NO',
                flight_date varchar path '../FLIGHT_DATE',
                litera varchar path '../LITERA',
                cshare varchar path '../CSHARE',
                conn_type varchar path '../CONN_TYPE',
                flight_tail_no varchar path '../TAIL_NO',
                route varchar path '../ROUTE',
                flt_kind varchar path '../FLT_KIND',
                transp_type varchar path '../TRANSP_TYPE',
                remark varchar path '../REMARK',
                actual_date varchar path '../ACTUAL_DATE',
                --leg
                leg_tail_no varchar path 'TAIL_NO',
                origin varchar path 'ORIGIN',
                destination varchar path 'DESTINATION',
                departure_plan varchar path 'DEPARTURE_PLAN',
                arrival_plan varchar path 'ARRIVAL_PLAN',
                departure_fact varchar path 'DEPARTURE_FACT',
                arrival_fact varchar path 'ARRIVAL_FACT',
                departure_plan_local varchar path 'DEPARTURE_PLAN_LOCAL',
                arrival_plan_local varchar path 'ARRIVAL_PLAN_LOCAL',
                departure_fact_local varchar path 'DEPARTURE_FACT_LOCAL',
                arrival_fact_local varchar path 'ARRIVAL_FACT_LOCAL',
                power_on varchar path 'POWER_ON',
                power_off varchar path 'POWER_OFF',
                on_surface varchar path 'ON_SURFACE',
                f_nr int path 'F_NR',
                c_nr int path 'C_NR',
                y_nr int path 'Y_NR',
                child_nr int path 'CHILD_NR',
                infant_nr int path 'INFANT_NR',
                crg_load int path 'CRG_LOAD',
                luggage_load int path 'LUGGAGE_LOAD',
                hand_luggage int path 'HAND_LUGGAGE',
                mail_load int path 'MAIL_LOAD',
                cabin_baggage int path 'CABIN_BAGGAGE',
                emptying int path 'EMPTYING',
                fueling int path 'FUELING',
                dep_remnant int path 'DEP_REMNANT',
                arr_remnant int path 'ARR_REMNANT',
                f_seats int path 'F_SEATS',
                c_seats int path 'C_SEATS',
                y_seats int path 'Y_SEATS',
                payload int path 'PAYLOAD',
                f_child int path 'F_CHILD',
                c_child int path 'C_CHILD',
                y_child int path 'Y_CHILD',
                f_infant int path 'F_INFANT',
                c_infant int path 'C_INFANT',
                y_infant int path 'Y_INFANT',
                distance int path 'DISTANCE',
                equipped_empty_kg int path 'EQUIPPED_EMPTY_KG',
                flight_task varchar path 'CREW_TASK/NO')

        where raw_meridian_sap_xml.id::integer = c_row.id;

        -- inserting new SAL data
        delete from dwh_presentation.fact_leg
        where source_code = 'SAL'
        and md5_leg in (select distinct md5_leg from dwh_staging.fact_leg_sal where source_code = 'SAL');

        insert into dwh_presentation.fact_leg(
            md5_flight, md5_leg, id_date, id_time, id_flight, id_board, id_origin, id_destination,
            source_code, source, flight_date, actual_date,
            departure_plan_local, departure_plan_utc, arrival_plan_local, arrival_plan_utc,
            departure_fact_local, departure_fact_utc, arrival_fact_local, arrival_fact_utc,
            power_on, power_off, on_surface,
            f_nr, c_nr, y_nr, child_nr, infant_nr, crg_load, luggage_load, hand_luggage, mail_load, cabin_baggage,
            emptying, fueling, dep_remnant, arr_remnant,
            f_seats_occupied, c_seats_occupied, y_seats_occupied, payload, f_child, c_child, y_child, f_infant, c_infant, y_infant, distance,
            equipped_empty_kg, flight_task)
        select distinct
            md5_flight, md5_leg, id_date, id_time, id_flight, id_board, id_origin, id_destination,
            source_code, source, flight_date, actual_date,
            departure_plan_local, departure_plan_utc, arrival_plan_local, arrival_plan_utc,
            departure_fact_local, departure_fact_utc, arrival_fact_local, arrival_fact_utc,
            power_on, power_off, on_surface,
            f_nr, c_nr, y_nr, child_nr, infant_nr, crg_load, luggage_load, hand_luggage, mail_load, cabin_baggage,
            emptying, fueling, dep_remnant, arr_remnant,
            f_seats_occupied, c_seats_occupied, y_seats_occupied, payload, f_child, c_child, y_child, f_infant, c_infant, y_infant, distance,
            equipped_empty_kg, flight_task
        from dwh_staging.fact_leg_sal;

        -- additional facts
        perform dwh_staging.sf_transform_meridian_xml_data_sal_segment(c_row.id);
        perform dwh_staging.sf_transform_meridian_xml_data_sal_crewtask(c_row.id);

        -- merging SAL + OPS
        update dwh_presentation.fact_leg
        set
            md5_flight = fls.md5_flight,
            md5_leg = fls.md5_leg,
            id_date = fls.id_date,
            id_time = fls.id_time,
            id_flight = dwh_staging.sf_dim_flight(fls.carrier_code, fls.subcarrier_code, fls.flight_no, fls.litera, fls.cshare, fls.conn_type, fls.route, fls.flt_kind, fls.transp_type, fls.remark, df.status, df.stc),
            id_board = fls.id_board,
            id_origin = fls.id_origin,
            id_destination = fls.id_destination,
            source_code = 'MERGED',
            source = '"' || dwh_presentation.fact_leg.source || '" & "' || fls.source || '"',
            flight_date = fls.flight_date,
            actual_date = fls.actual_date,
            departure_plan_local = fls.departure_plan_local,
            departure_plan_utc = fls.departure_plan_utc,
            arrival_plan_local = fls.arrival_plan_local,
            arrival_plan_utc = fls.arrival_plan_utc,
            departure_fact_local = fls.departure_fact_local,
            departure_fact_utc = fls.departure_fact_utc,
            arrival_fact_local = fls.arrival_fact_local,
            arrival_fact_utc = fls.arrival_fact_utc,
            power_on = fls.power_on,
            power_off = fls.power_off,
            on_surface = fls.on_surface,
            f_nr = fls.f_nr,
            c_nr = fls.c_nr,
            y_nr = fls.y_nr,
            child_nr = fls.child_nr,
            infant_nr = fls.infant_nr,
            crg_load = fls.crg_load,
            luggage_load = fls.luggage_load,
            hand_luggage = fls.hand_luggage,
            mail_load = fls.mail_load,
            cabin_baggage = fls.cabin_baggage,
            emptying = fls.emptying,
            fueling = fls.fueling,
            dep_remnant = fls.dep_remnant,
            arr_remnant = fls.arr_remnant,
            f_seats_occupied = fls.f_seats_occupied,
            c_seats_occupied = fls.c_seats_occupied,
            y_seats_occupied = fls.y_seats_occupied,
            payload = fls.payload,
            f_child = fls.f_child,
            c_child = fls.c_child,
            y_child = fls.y_child,
            f_infant = fls.f_infant,
            c_infant = fls.c_infant,
            y_infant = fls.y_infant,
            distance = fls.distance,
            equipped_empty_kg = fls.equipped_empty_kg,
            flight_task = fls.flight_task
        from dwh_staging.fact_leg_sal fls
        left join dwh_presentation.fact_leg fl on fl.md5_leg = fls.md5_leg
            and fl.id_origin = fls.id_origin
            and fl.id_destination = fls.id_destination
        left join dwh_presentation.dim_flight df on df.id = fl.id_flight
        where 1=1
        and fl.source_code = 'OPS'
        and dwh_presentation.fact_leg.source_code = 'OPS'
        and dwh_presentation.fact_leg.md5_leg = fls.md5_leg
        and dwh_presentation.fact_leg.id_origin = fls.id_origin
        and dwh_presentation.fact_leg.id_destination = fls.id_destination;

        -- clearing already merged SAL data
        delete from dwh_presentation.fact_leg
        where id in (
            select s.id
            from (select id, md5_leg, id_origin, id_destination from dwh_presentation.fact_leg where source_code = 'SAL') as s
            inner join (select id, md5_leg, id_origin, id_destination from dwh_presentation.fact_leg where source_code = 'MERGED') as m on s.md5_leg = m.md5_leg and s.id_origin = m.id_origin and s.id_destination = m.id_destination);


        -- finalisation
        update dwh_staging.raw_meridian_sap_xml set date_processed = now() where id = c_row.id;
        raise notice 'SAL raw file processing with id = % complete', c_row.id;

    end loop;

    -- trigger extermal data marts recalculations
    perform dwh_staging.sf_transform_skytrac_takeoff_landing_recalc();

end$$;