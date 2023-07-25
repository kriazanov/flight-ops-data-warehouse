create or replace function dwh_staging.sf_transform_meridian_xml_data_sal_crewtask(p_source_id int)
returns void
language plpgsql
as $$
begin
    truncate table dwh_staging.fact_crewtask;

    insert into dwh_staging.fact_crewtask
    select md5_flight, id_date,id_time, id_task_date, id_flight, id_board, id_origin, id_destination, id_crew_task_member, id_crew_task_division, id_crew_task_member_division, crew_task_no
    from(
        select distinct
            --id's
            md5(coalesce(flight_no, '')::varchar || coalesce(flight_tail_no, '')::varchar || coalesce(to_char(to_timestamp(nullif(trim(flight_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '') || coalesce(to_char(to_timestamp(nullif(trim(actual_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '')) as md5_flight,
            dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int)  as id_date,
            dwh_staging.sf_dim_time(to_char(to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY HH24:MI'), 'HH24MI')::int) as id_time,
            dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(crew_task_date, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int)  as id_task_date,
            dwh_staging.sf_dim_flight(carrier, subcarrier_code, flight_no, litera, cshare, conn_type, route, flt_kind, transp_type, remark, null,null) as id_flight,
            dwh_staging.sf_dim_board(false, flight_tail_no, null, null) as id_board,
            dwh_staging.sf_dim_station(false, origin, origin, null, null,null) as id_origin,
            dwh_staging.sf_dim_station(false, destination, destination, null, null, null) as id_destination,
            dwh_staging.sf_dim_crew_task_member(member_no, member_full_name, member_role) as id_crew_task_member,
            dwh_staging.sf_dim_crew_task_division(crew_task_division_no, crew_task_division_name) as id_crew_task_division,
            dwh_staging.sf_dim_crew_task_division(member_division_no, member_division_name) as id_crew_task_member_division,

            -- crew task
            crew_task_no

        from dwh_staging.raw_meridian_sap_xml,
            xmltable('//PERFORMED_FLIGHTS/FLIGHT/LEG/CREW_TASK/MEMBERS/MEMBER' passing file_content columns
                -- flight
                subcarrier_code varchar path '../../../../SUBCARRIER_CODE',
                carrier varchar path '../../../../CARRIER',
                flight_no varchar path '../../../../FLIGHT_NO',
                flight_date varchar path '../../../../FLIGHT_DATE',
                litera varchar path '../../../../LITERA',
                cshare varchar path '../../../../CSHARE',
                conn_type varchar path '../../../../CONN_TYPE',
                flight_tail_no varchar path '../../../../TAIL_NO',
                route varchar path '../../../../ROUTE',
                flt_kind varchar path '../../../../FLT_KIND',
                transp_type varchar path '../../../../TRANSP_TYPE',
                remark varchar path '../../../../REMARK',
                actual_date varchar path '../../../../ACTUAL_DATE',
                origin varchar path '../../../ORIGIN',
                destination varchar path '../../../DESTINATION',

                -- crew task
                crew_task_no varchar path '../../NO',
                crew_task_date varchar path '../../DATE',
                crew_task_division_no varchar path '../../DIVISION/NO',
                crew_task_division_name varchar path '../../DIVISION/NAME',
                member_no varchar path 'NO',
                member_full_name varchar path 'FULL_NAME',
                member_division_no varchar path 'DIVISION/NO',
                member_division_name varchar path 'DIVISION/NAME',
                member_role varchar path '../ROLE/FLIGHT_ROLE')

        where raw_meridian_sap_xml.id = p_source_id

        union

        select distinct
            --id's
            md5(flight_no::varchar || coalesce(to_char(to_timestamp(nullif(trim(flight_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '') || coalesce(to_char(to_timestamp(nullif(trim(flight_date), ''), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::varchar, '')) as md5_flight,
            dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int)  as id_date,
            dwh_staging.sf_dim_time(to_char(to_timestamp(nullif(flight_date, ' '), 'DD.MM.YYYY HH24:MI'), 'HH24MI')::int) as id_time,
            dwh_staging.sf_dim_date(to_char(to_timestamp(nullif(crew_task_date, ' '), 'DD.MM.YYYY HH24:MI'), 'YYYYMMDD')::int)  as id_task_date,
            dwh_staging.sf_dim_flight(carrier, subcarrier_code, flight_no, litera, cshare, conn_type, route, flt_kind, transp_type, remark, null,null) as id_flight,
            dwh_staging.sf_dim_board(false, flight_tail_no, null, null) as id_board,
            dwh_staging.sf_dim_station(false, origin, origin, null, null,null) as id_origin,
            dwh_staging.sf_dim_station(false, destination, destination, null, null, null) as id_destination,
            dwh_staging.sf_dim_crew_task_member(member_no, member_full_name, member_role) as id_crew_task_member,
            dwh_staging.sf_dim_crew_task_division(crew_task_division_no, crew_task_division_name) as id_crew_task_division,
            dwh_staging.sf_dim_crew_task_division(member_division_no, member_division_name) as id_crew_task_member_division,

            -- crew task
            crew_task_no

        from dwh_staging.raw_meridian_sap_xml,
            xmltable('//PERFORMED_FLIGHTS/FLIGHT/LEG/STEWARD_TASK/MEMBERS/MEMBER' passing file_content columns
                -- flight
                subcarrier_code varchar path '../../../../SUBCARRIER_CODE',
                carrier varchar path '../../../../CARRIER',
                flight_no varchar path '../../../../FLIGHT_NO',
                flight_date varchar path '../../../../FLIGHT_DATE',
                litera varchar path '../../../../LITERA',
                cshare varchar path '../../../../CSHARE',
                conn_type varchar path '../../../../CONN_TYPE',
                flight_tail_no varchar path '../../../../TAIL_NO',
                route varchar path '../../../../ROUTE',
                flt_kind varchar path '../../../../FLT_KIND',
                transp_type varchar path '../../../../TRANSP_TYPE',
                remark varchar path '../../../../REMARK',
                actual_date varchar path '../../../../ACTUAL_DATE',
                origin varchar path '../../../ORIGIN',
                destination varchar path '../../../DESTINATION',

                -- crew task
                crew_task_no varchar path '../../NO',
                crew_task_date varchar path '../../DATE',
                crew_task_division_no varchar path '../../DIVISION/NO',
                crew_task_division_name varchar path '../../DIVISION/NAME',
                member_no varchar path 'NO',
                member_full_name varchar path 'FULL_NAME',
                member_division_no varchar path 'DIVISION/NO',
                member_division_name varchar path 'DIVISION/NAME',
                member_role varchar path '../ROLE/FLIGHT_ROLE')

        where raw_meridian_sap_xml.id = p_source_id
        ) q;

    delete from dwh_presentation.fact_crewtask
    where md5_flight in (select distinct md5_flight from dwh_staging.fact_crewtask);

    insert into dwh_presentation.fact_crewtask(
        md5_flight, id_date, id_time, id_task_date, id_flight, id_board, id_origin, id_destination,
        id_crew_task_member, id_crew_task_division, id_crew_task_member_division, crew_task_no)
    select
        md5_flight, id_date, id_time, id_task_date, id_flight, id_board, id_origin, id_destination,
        id_crew_task_member, id_crew_task_division, id_crew_task_member_division, crew_task_no
    from dwh_staging.fact_crewtask;

end
$$