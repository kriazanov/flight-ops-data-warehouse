create or replace function dwh_staging.sf_transform_meridian_xml_data_sal_segment(p_source_id int)
returns void
language plpgsql
as $$
begin
    truncate table dwh_staging.fact_segment;

    insert into dwh_staging.fact_segment
    select distinct
        --id's
        md5(coalesce(flight_no, '')::varchar || coalesce(flight_tail_no, '')::varchar || coalesce(to_char(to_timestamp(nullif(trim(flight_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '') || coalesce(to_char(to_timestamp(nullif(trim(actual_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '')) as md5_flight,
        dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int)  as id_date,
        dwh_staging.sf_dim_time(to_char(to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY HH24:MI'), 'HH24MI')::int) as id_time,
        dwh_staging.sf_dim_flight(carrier, subcarrier_code, flight_no, litera, cshare, conn_type, route, flt_kind, transp_type, remark, null,null) as id_flight,
        dwh_staging.sf_dim_board(false, flight_tail_no, null, null) as id_board,
        dwh_staging.sf_dim_station(false, city_from, city_from, null, null,null) as id_city_from,
        dwh_staging.sf_dim_station(false, city_to, city_to, null, null, null) as id_city_to,
        dwh_staging.sf_dim_station(false, origin, origin, null, null,null) as id_origin,
        dwh_staging.sf_dim_station(false, destination, destination, null, null, null) as id_destination,
        dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(actual_date, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int) as id_date_actual,

        --segment
        f_adult, f_child, f_infant,
        c_adult, c_child, c_infant,
        y_adult, y_child, y_infant,
        luggage_kg, ex_luggage_kg,
        cargo_kg, mail_kg,
        null as flight_task

    from dwh_staging.raw_meridian_sap_xml,
        xmltable('//PERFORMED_FLIGHTS/FLIGHT/SEGMENT' passing file_content columns
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

            --segment
            city_from varchar path 'CITY_FROM',
            city_to varchar path 'CITY_TO',
            origin varchar path 'ORIGIN',
            destination varchar path 'DESTINATION',
            f_adult integer path 'F_CLASS/ADULT',
            f_child integer path 'F_CLASS/CHILD',
            f_infant integer path 'F_CLASS/INFANT',
            c_adult integer path 'C_CLASS/ADULT',
            c_child integer path 'C_CLASS/CHILD',
            c_infant integer path 'C_CLASS/INFANT',
            y_adult integer path 'Y_CLASS/ADULT',
            y_child integer path 'Y_CLASS/CHILD',
            y_infant integer path 'Y_CLASS/INFANT',
            luggage_kg float path 'LUGGAGE_KG',
            ex_luggage_kg float path 'EX_LUGGAGE_KG',
            cargo_kg float path 'CARGO_KG',
            mail_kg float path 'MAIL_KG')

    where raw_meridian_sap_xml.id = p_source_id;

    update dwh_staging.fact_segment fs
    set flight_task = fl.flight_task
    from dwh_presentation.fact_leg fl
    where fs.md5_flight = fl.md5_flight;

    update dwh_staging.fact_segment fs
    set flight_task = fl.flight_task
    from dwh_presentation.fact_leg fl
    where fs.md5_flight = fl.md5_flight
    and fs.id_origin = fl.id_origin
    and fs.id_destination = fl.id_destination;

    delete from dwh_presentation.fact_segment
    where md5_flight in (select distinct md5_flight from dwh_staging.fact_segment);

    insert into dwh_presentation.fact_segment(
        md5_flight, id_date, id_time, id_flight, id_board, id_city_from, id_city_to, id_origin, id_destination, id_date_actual,
        f_adult, f_child, f_infant, c_adult, c_child, c_infant, y_adult, y_child, y_infant, luggage_kg, ex_luggage_kg, cargo_kg, mail_kg, flight_task)
    select
        md5_flight, id_date, id_time, id_flight, id_board, id_city_from, id_city_to, id_origin, id_destination, id_date_actual,
        f_adult, f_child, f_infant, c_adult, c_child, c_infant, y_adult, y_child, y_infant, luggage_kg, ex_luggage_kg, cargo_kg, mail_kg, flight_task
    from dwh_staging.fact_segment;

end$$;