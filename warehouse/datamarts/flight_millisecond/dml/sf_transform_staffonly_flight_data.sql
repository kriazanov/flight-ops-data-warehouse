create or replace function dwh_staging.sf_transform_staffonly_flight_data()
returns void
language plpgsql
as $$
declare
    POUND_CONVERSION_RATE float = 2.2046;
begin
    -- preparations
    truncate table dwh_staging.fact_flight_millisecond;
    truncate table dwh_staging.bridge_fact_flight_millisecond_to_fact_leg;

    -- raw data processing
    insert into dwh_staging.fact_flight_millisecond
    select distinct
        null::int as id_flight,
        db.id as id_board,
        to_char(end_record, 'YYYYMMDD')::int as id_date,
        to_char(param_key, 'HH24MI')::int as id_time,
        dfp.id as id_flight_parameter,
        null::int as id_origin,
        null::int as id_destination,
        rsfd.end_record,
        rsfd.param_key,
        rsfd.param_value,
        case when rsfd.param_no = flight4.param_id then rsfd.param_value end as flight_no_1,
        case when rsfd.param_no = flight3.param_id then rsfd.param_value end as flight_no_2,
        case when rsfd.param_no = flight2.param_id then rsfd.param_value end as flight_no_3,
        case when rsfd.param_no = flight1.param_id then rsfd.param_value end as flight_no_4,
        null as pilot_input_flight_no,
        case when rsfd.param_no = gt.param_id then rsfd.param_value end as fuel_remaining_total,
        case when rsfd.param_no = fqty_m_l.param_id then rsfd.param_value end as fuel_tank_qty_left_wing,
        case when rsfd.param_no = fqty_m_r.param_id then rsfd.param_value end as fuel_tank_qty_right_wing,
        case when rsfd.param_no = fqty_ctr.param_id then rsfd.param_value end as fuel_tank_qty_center,
        case when rsfd.param_no = weight_pounds.param_id then rsfd.param_value end as aircraft_weight_pounds,
        case when rsfd.param_no = weight_tons.param_id then rsfd.param_value end as aircraft_weight_tons,
        case when rsfd.param_no = weight_without_fuel_tons.param_id then rsfd.param_value end as aircraft_weight_without_fuel_tons

    from dwh_staging.raw_staffonly_files_decrypted rsfd
    left join dwh_presentation.dim_board db on db.tail_no = rsfd.tail_no
    left join dwh_presentation.dim_flight_parameter dfp on dfp.tail_no = rsfd.tail_no and dfp.param_id = rsfd.param_no
    -- pilot input flight no
    left join dwh_presentation.dim_flight_parameter flight1 on flight1.tail_no = rsfd.tail_no and flight1.param_id = rsfd.param_no and flight1.param_code in ('FLIGHT1', 'Рейс1')
    left join dwh_presentation.dim_flight_parameter flight2 on flight2.tail_no = rsfd.tail_no and flight2.param_id = rsfd.param_no and flight2.param_code in ('FLIGHT2', 'Рейс2')
    left join dwh_presentation.dim_flight_parameter flight3 on flight3.tail_no = rsfd.tail_no and flight3.param_id = rsfd.param_no and flight3.param_code in ('FLIGHT3', 'Рейс3')
    left join dwh_presentation.dim_flight_parameter flight4 on flight4.tail_no = rsfd.tail_no and flight4.param_id = rsfd.param_no and flight4.param_code in ('FLIGHT4', 'Рейс4')
    -- fuel data
    left join dwh_presentation.dim_flight_parameter gt on gt.tail_no = rsfd.tail_no and gt.param_id = rsfd.param_no and gt.param_code = 'GT'
    left join dwh_presentation.dim_flight_parameter fqty_m_l on fqty_m_l.tail_no = rsfd.tail_no and fqty_m_l.param_id = rsfd.param_no and fqty_m_l.param_code in ('FQTY.M.L', 'FQ.L', 'GT1')
    left join dwh_presentation.dim_flight_parameter fqty_m_r on fqty_m_r.tail_no = rsfd.tail_no and fqty_m_r.param_id = rsfd.param_no and fqty_m_r.param_code in ('FQTY.M.R', 'FQ.R', 'GT2')
    left join dwh_presentation.dim_flight_parameter fqty_ctr on fqty_ctr.tail_no = rsfd.tail_no and fqty_ctr.param_id = rsfd.param_no and fqty_ctr.param_code in ('FQTY.CTR', 'FQ.C')
    -- aircraft weight data
    left join dwh_presentation.dim_flight_parameter weight_pounds on weight_pounds.tail_no = rsfd.tail_no and weight_pounds.param_id = rsfd.param_no and weight_pounds.param_code in ('Weight', 'Масса.f', 'Масса.ф')
    left join dwh_presentation.dim_flight_parameter weight_tons on weight_tons.tail_no = rsfd.tail_no and weight_tons.param_id = rsfd.param_no and weight_tons.param_code in ('Масса')
    left join dwh_presentation.dim_flight_parameter weight_without_fuel_tons on weight_without_fuel_tons.tail_no = rsfd.tail_no and weight_without_fuel_tons.param_id = rsfd.param_no and weight_without_fuel_tons.param_code in ('Масса.п')

    where 1=1
    and dfp.param_id is not null
    order by db.id, rsfd.end_record, rsfd.param_key;

    -- retrieving flight number flom pilot input
    update dwh_staging.fact_flight_millisecond ffm
    set pilot_input_flight_no = (f1.flight_no_1::varchar || f2.flight_no_2::varchar || f3.flight_no_3::varchar || f4.flight_no_4::varchar)::int::varchar

    from (select distinct id_board, end_record from dwh_staging.fact_flight_millisecond ffm) base
    left join (
        select distinct id_board, end_record, percentile_disc(0.5) within group(order by param_key) as param_key
        from dwh_staging.fact_flight_millisecond
        where flight_no_4 is not null
        group by id_board, end_record) f4_bridge on f4_bridge.id_board = base.id_board and f4_bridge.end_record = base.end_record
    left join dwh_staging.fact_flight_millisecond as f4 on f4.id_board = f4_bridge.id_board
        and f4.end_record = f4_bridge.end_record
        and f4.param_key = f4_bridge.param_key
        and f4.flight_no_4 is not null
    left join (
        select distinct id_board, end_record, percentile_disc(0.5) within group(order by param_key) as param_key
        from dwh_staging.fact_flight_millisecond
        where flight_no_3 is not null
        group by id_board, end_record) f3_bridge on f3_bridge.id_board = base.id_board and f3_bridge.end_record = base.end_record
    left join dwh_staging.fact_flight_millisecond as f3 on f3.id_board = f3_bridge.id_board
        and f3.end_record = f3_bridge.end_record
        and f3.param_key = f3_bridge.param_key and f3.flight_no_3 is not null
    left join (
        select distinct id_board, end_record, percentile_disc(0.5) within group(order by param_key) as param_key
        from dwh_staging.fact_flight_millisecond
        where flight_no_2 is not null
        group by id_board, end_record) f2_bridge on f2_bridge.id_board = base.id_board and f2_bridge.end_record = base.end_record
    left join dwh_staging.fact_flight_millisecond as f2 on f2.id_board = f2_bridge.id_board
        and f2.end_record = f2_bridge.end_record
        and f2.param_key = f2_bridge.param_key
        and f2.flight_no_2 is not null
    left join (
        select distinct id_board, end_record, percentile_disc(0.5) within group(order by param_key) as param_key
        from dwh_staging.fact_flight_millisecond
        where flight_no_1 is not null
        group by id_board, end_record) f1_bridge on f1_bridge.id_board = base.id_board and f1_bridge.end_record = base.end_record
    left join dwh_staging.fact_flight_millisecond as f1 on f1.id_board = f1_bridge.id_board
        and f1.end_record = f1_bridge.end_record
        and f1.param_key = f1_bridge.param_key and f1.flight_no_1 is not null

    where ffm.id_board = base.id_board
      and ffm.end_record = base.end_record;

    -- building bridge to fact_leg
    insert into dwh_staging.bridge_fact_flight_millisecond_to_fact_leg(
        id_board, end_record, md5_leg, id_flight, id_origin, id_destination,
        fuel_qty_on_flight_start_record, fuel_qty_on_flight_finish_record,
        aircraft_weight_pounds_start_record, aircraft_weight_pounds_finish_record,
        aircraft_weight_tons_start_record, aircraft_weight_tons_finish_record,
        aircraft_weight_without_fuel_tons_start_record, aircraft_weight_without_fuel_tons_finish_record)
    select
        q.id_board, q.end_record, q.md5_leg, fl.id_flight, fl.id_origin, fl.id_destination,
        q.fuel_qty_on_flight_start_record, q.fuel_qty_on_flight_finish_record,
        q.aircraft_weight_pounds_start_record, q.aircraft_weight_pounds_finish_record,
        q.aircraft_weight_tons_start_record, q.aircraft_weight_tons_finish_record,
        q.aircraft_weight_without_fuel_tons_start_record, q.aircraft_weight_without_fuel_tons_finish_record
    from (
        select
            base.id_board,
            base.end_record,
            (select fl.md5_leg
            from dwh_presentation.fact_leg fl
            left join dwh_presentation.dim_flight df on df.id = fl.id_flight
            where fl.id_board = base.id_board
            and fl.arrival_fact_utc <= base.end_record
            and df.flight_no = base.pilot_input_flight_no
            order by arrival_fact_utc desc
            limit 1) as md5_leg,

            --fuel data
            case
                when fuel_remaining_total_begin.fuel_remaining_total is null
                then (
                    coalesce(fuel_tank_qty_left_wing_begin.fuel_tank_qty_left_wing, 0) +
                    coalesce(fuel_tank_qty_right_wing_begin.fuel_tank_qty_right_wing, 0) +
                    coalesce(fuel_tank_qty_center_begin.fuel_tank_qty_center, 0)) / POUND_CONVERSION_RATE
                else fuel_remaining_total_begin.fuel_remaining_total
            end as fuel_qty_on_flight_start_record,

            case
                when fuel_remaining_total_end.fuel_remaining_total is null
                then (
                    coalesce(fuel_tank_qty_left_wing_end.fuel_tank_qty_left_wing, 0) +
                    coalesce(fuel_tank_qty_right_wing_end.fuel_tank_qty_right_wing, 0) +
                    coalesce(fuel_tank_qty_center_end.fuel_tank_qty_center, 0)) / POUND_CONVERSION_RATE
                else fuel_remaining_total_end.fuel_remaining_total
            end as fuel_qty_on_flight_finish_record,

            --aircraft weight data
            aircraft_weight_pounds_begin.aircraft_weight_pounds as aircraft_weight_pounds_start_record,
            aircraft_weight_pounds_end.aircraft_weight_pounds as aircraft_weight_pounds_finish_record,
            aircraft_weight_tons_begin.aircraft_weight_tons as aircraft_weight_tons_start_record,
            aircraft_weight_tons_end.aircraft_weight_tons as aircraft_weight_tons_finish_record,
            aircraft_weight_without_fuel_tons_begin.aircraft_weight_without_fuel_tons as aircraft_weight_without_fuel_tons_start_record,
            aircraft_weight_without_fuel_tons_end.aircraft_weight_without_fuel_tons as aircraft_weight_without_fuel_tons_finish_record

        from (select distinct id_board, end_record, pilot_input_flight_no from dwh_staging.fact_flight_millisecond) base

        --fuel_remaining_total_begin
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_remaining_total is not null
            group by id_board, end_record) bridge_fuel_remaining_total_begin on 1=1
                and bridge_fuel_remaining_total_begin.id_board = base.id_board
                and bridge_fuel_remaining_total_begin.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_remaining_total_begin on 1=1
            and fuel_remaining_total_begin.id_board = bridge_fuel_remaining_total_begin.id_board
            and fuel_remaining_total_begin.end_record = bridge_fuel_remaining_total_begin.end_record
            and fuel_remaining_total_begin.param_key = bridge_fuel_remaining_total_begin.param_key
            and fuel_remaining_total_begin.fuel_remaining_total is not null

        --fuel_remaining_total_end
        left join (
            select distinct id_board, end_record, max(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_remaining_total is not null
            group by id_board, end_record) bridge_fuel_remaining_total_end on 1=1
                and bridge_fuel_remaining_total_end.id_board = base.id_board
                and bridge_fuel_remaining_total_end.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_remaining_total_end on 1=1
            and fuel_remaining_total_end.id_board = bridge_fuel_remaining_total_end.id_board
            and fuel_remaining_total_end.end_record = bridge_fuel_remaining_total_end.end_record
            and fuel_remaining_total_end.param_key = bridge_fuel_remaining_total_end.param_key
            and fuel_remaining_total_end.fuel_remaining_total is not null

        --fuel_tank_qty_left_wing_begin
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_tank_qty_left_wing is not null
            group by id_board, end_record) bridge_fuel_tank_qty_left_wing_begin on 1=1
                and bridge_fuel_tank_qty_left_wing_begin.id_board = base.id_board
                and bridge_fuel_tank_qty_left_wing_begin.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_tank_qty_left_wing_begin on 1=1
            and fuel_tank_qty_left_wing_begin.id_board = bridge_fuel_tank_qty_left_wing_begin.id_board
            and fuel_tank_qty_left_wing_begin.end_record = bridge_fuel_tank_qty_left_wing_begin.end_record
            and fuel_tank_qty_left_wing_begin.param_key = bridge_fuel_tank_qty_left_wing_begin.param_key
            and fuel_tank_qty_left_wing_begin.fuel_tank_qty_left_wing is not null

        --fuel_tank_qty_left_wing_end
        left join (
            select distinct id_board, end_record, max(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_tank_qty_left_wing is not null
            group by id_board, end_record) bridge_fuel_tank_qty_left_wing_end on 1=1
                and bridge_fuel_tank_qty_left_wing_end.id_board = base.id_board
                and bridge_fuel_tank_qty_left_wing_end.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_tank_qty_left_wing_end on 1=1
            and fuel_tank_qty_left_wing_end.id_board = bridge_fuel_tank_qty_left_wing_end.id_board
            and fuel_tank_qty_left_wing_end.end_record = bridge_fuel_tank_qty_left_wing_end.end_record
            and fuel_tank_qty_left_wing_end.param_key = bridge_fuel_tank_qty_left_wing_end.param_key
            and fuel_tank_qty_left_wing_end.fuel_tank_qty_left_wing is not null

        --fuel_tank_qty_right_wing_begin
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_tank_qty_right_wing is not null
            group by id_board, end_record) bridge_fuel_tank_qty_right_wing_begin on 1=1
                and bridge_fuel_tank_qty_right_wing_begin.id_board = base.id_board
                and bridge_fuel_tank_qty_right_wing_begin.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_tank_qty_right_wing_begin on 1=1
            and fuel_tank_qty_right_wing_begin.id_board = bridge_fuel_tank_qty_right_wing_begin.id_board
            and fuel_tank_qty_right_wing_begin.end_record = bridge_fuel_tank_qty_right_wing_begin.end_record
            and fuel_tank_qty_right_wing_begin.param_key = bridge_fuel_tank_qty_right_wing_begin.param_key
            and fuel_tank_qty_right_wing_begin.fuel_tank_qty_right_wing is not null

        --fuel_tank_qty_right_wing_end
        left join (
            select distinct id_board, end_record, max(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_tank_qty_right_wing is not null
            group by id_board, end_record) bridge_fuel_tank_qty_right_wing_end on 1=1
                and bridge_fuel_tank_qty_right_wing_end.id_board = base.id_board
                and bridge_fuel_tank_qty_right_wing_end.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_tank_qty_right_wing_end on 1=1
            and fuel_tank_qty_right_wing_end.id_board = bridge_fuel_tank_qty_right_wing_end.id_board
            and fuel_tank_qty_right_wing_end.end_record = bridge_fuel_tank_qty_right_wing_end.end_record
            and fuel_tank_qty_right_wing_end.param_key = bridge_fuel_tank_qty_right_wing_end.param_key
            and fuel_tank_qty_right_wing_end.fuel_tank_qty_right_wing is not null

        --fuel_tank_qty_center_begin
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_tank_qty_center is not null
            group by id_board, end_record) bridge_fuel_tank_qty_center_begin on 1=1
                and bridge_fuel_tank_qty_center_begin.id_board = base.id_board
                and bridge_fuel_tank_qty_center_begin.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_tank_qty_center_begin on 1=1
            and fuel_tank_qty_center_begin.id_board = bridge_fuel_tank_qty_center_begin.id_board
            and fuel_tank_qty_center_begin.end_record = bridge_fuel_tank_qty_center_begin.end_record
            and fuel_tank_qty_center_begin.param_key = bridge_fuel_tank_qty_center_begin.param_key
            and fuel_tank_qty_center_begin.fuel_tank_qty_center is not null

        --fuel_tank_qty_center_end
        left join (
            select distinct id_board, end_record, max(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.fuel_tank_qty_center is not null
            group by id_board, end_record) bridge_fuel_tank_qty_center_end on 1=1
                and bridge_fuel_tank_qty_center_end.id_board = base.id_board
                and bridge_fuel_tank_qty_center_end.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as fuel_tank_qty_center_end on 1=1
            and fuel_tank_qty_center_end.id_board = bridge_fuel_tank_qty_center_end.id_board
            and fuel_tank_qty_center_end.end_record = bridge_fuel_tank_qty_center_end.end_record
            and fuel_tank_qty_center_end.param_key = bridge_fuel_tank_qty_center_end.param_key
            and fuel_tank_qty_center_end.fuel_tank_qty_center is not null

        --aircraft_weight_pounds_start
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.aircraft_weight_pounds is not null
            group by id_board, end_record) bridge_aircraft_weight_pounds_begin on 1=1
                and bridge_aircraft_weight_pounds_begin.id_board = base.id_board
                and bridge_aircraft_weight_pounds_begin.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as aircraft_weight_pounds_begin on 1=1
            and aircraft_weight_pounds_begin.id_board = bridge_aircraft_weight_pounds_begin.id_board
            and aircraft_weight_pounds_begin.end_record = bridge_aircraft_weight_pounds_begin.end_record
            and aircraft_weight_pounds_begin.param_key = bridge_aircraft_weight_pounds_begin.param_key
            and aircraft_weight_pounds_begin.aircraft_weight_pounds is not null

        --aircraft_weight_pounds_finish
        left join (
            select distinct id_board, end_record, max(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.aircraft_weight_pounds is not null
            group by id_board, end_record) bridge_aircraft_weight_pounds_end on 1=1
                and bridge_aircraft_weight_pounds_end.id_board = base.id_board
                and bridge_aircraft_weight_pounds_end.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as aircraft_weight_pounds_end on 1=1
            and aircraft_weight_pounds_end.id_board = bridge_aircraft_weight_pounds_end.id_board and
            and aircraft_weight_pounds_end.end_record = bridge_aircraft_weight_pounds_end.end_record and
            and aircraft_weight_pounds_end.param_key = bridge_aircraft_weight_pounds_end.param_key and
            and aircraft_weight_pounds_end.aircraft_weight_pounds is not null

        --aircraft_weight_tons_start
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.aircraft_weight_tons is not null
            group by id_board, end_record) bridge_aircraft_weight_tons_begin on 1=1
                and bridge_aircraft_weight_tons_begin.id_board = base.id_board
                and bridge_aircraft_weight_tons_begin.end_record = base.end_record

        left join dwh_staging.fact_flight_millisecond as aircraft_weight_tons_begin on 1=1
            and aircraft_weight_tons_begin.id_board = bridge_aircraft_weight_tons_begin.id_board
            and aircraft_weight_tons_begin.end_record = bridge_aircraft_weight_tons_begin.end_record
            and aircraft_weight_tons_begin.param_key = bridge_aircraft_weight_tons_begin.param_key
            and aircraft_weight_tons_begin.aircraft_weight_tons is not null

        --aircraft_weight_tons_finish
        left join (
            select distinct id_board, end_record, max(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.aircraft_weight_tons is not null
            group by id_board, end_record) bridge_aircraft_weight_tons_end on 1=1
                and bridge_aircraft_weight_tons_end.id_board = base.id_board
                and bridge_aircraft_weight_tons_end.end_record = base.end_record
        left join dwh_staging.fact_flight_millisecond as aircraft_weight_tons_end on 1=1
            and aircraft_weight_tons_end.id_board = bridge_aircraft_weight_tons_end.id_board
            and aircraft_weight_tons_end.end_record = bridge_aircraft_weight_tons_end.end_record
            and aircraft_weight_tons_end.param_key = bridge_aircraft_weight_tons_end.param_key
            and aircraft_weight_tons_end.aircraft_weight_tons is not null

        --aircraft_weight_without_fuel_tons_start
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.aircraft_weight_tons is not null
            group by id_board, end_record) bridge_aircraft_weight_without_fuel_tons_begin on 1=1
                and bridge_aircraft_weight_without_fuel_tons_begin.id_board = base.id_board
                and bridge_aircraft_weight_without_fuel_tons_begin.end_record = base.end_record
        left join dwh_staging.fact_flight_millisecond as aircraft_weight_without_fuel_tons_begin on 1=1
            and aircraft_weight_without_fuel_tons_begin.id_board = bridge_aircraft_weight_without_fuel_tons_begin.id_board
            and aircraft_weight_without_fuel_tons_begin.end_record = bridge_aircraft_weight_without_fuel_tons_begin.end_record
            and aircraft_weight_without_fuel_tons_begin.param_key = bridge_aircraft_weight_without_fuel_tons_begin.param_key
            and aircraft_weight_without_fuel_tons_begin.aircraft_weight_tons is not null
        --aircraft_weight_without_fuel_tons_finish
        left join (
            select distinct id_board, end_record, min(param_key) as param_key
            from dwh_staging.fact_flight_millisecond
            where fact_flight_millisecond.aircraft_weight_tons is not null
            group by id_board, end_record) bridge_aircraft_weight_without_fuel_tons_end on 1=1
                and bridge_aircraft_weight_without_fuel_tons_end.id_board = base.id_board
                and bridge_aircraft_weight_without_fuel_tons_end.end_record = base.end_record
        left join dwh_staging.fact_flight_millisecond as aircraft_weight_without_fuel_tons_end on 1=1
            and aircraft_weight_without_fuel_tons_end.id_board = bridge_aircraft_weight_without_fuel_tons_end.id_board
            and aircraft_weight_without_fuel_tons_end.end_record = bridge_aircraft_weight_without_fuel_tons_end.end_record
            and aircraft_weight_without_fuel_tons_end.param_key = bridge_aircraft_weight_without_fuel_tons_end.param_key
            and aircraft_weight_without_fuel_tons_end.aircraft_weight_tons is not null) q
        left join dwh_presentation.fact_leg fl on fl.md5_leg = q.md5_leg;

    delete from dwh_presentation.bridge_fact_flight_millisecond_to_fact_leg
    where md5_leg in (select md5_leg from dwh_staging.bridge_fact_flight_millisecond_to_fact_leg);

    insert into dwh_presentation.bridge_fact_flight_millisecond_to_fact_leg
    select * from dwh_staging.bridge_fact_flight_millisecond_to_fact_leg;

    -- updating flight id's
    update dwh_staging.fact_flight_millisecond u
    set id_flight = b.id_flight, id_origin = b.id_origin, id_destination = b.id_destination
    from dwh_staging.bridge_fact_flight_millisecond_to_fact_leg b
    where u.id_board = b.id_board and u.end_record = u.end_record;

    -- populate presentation fact table
    delete from dwh_presentation.fact_flight_millisecond
    where id_board::varchar || param_key::varchar in (select id_board::varchar || param_key::varchar from dwh_staging.fact_flight_millisecond);

    insert into dwh_presentation.fact_flight_millisecond(
        id_flight, id_board, id_date, id_time, id_flight_parameter, end_record, param_key, param_value,
        pilot_input_flight_no, flight_no_1, flight_no_2, flight_no_3, flight_no_4, fuel_remaining_total,
        fuel_tank_qty_left_wing, fuel_tank_qty_right_wing, fuel_tank_qty_center)
    select
        id_flight, id_board, id_date, id_time, id_flight_parameter, end_record, param_key, param_value,
        pilot_input_flight_no, flight_no_1, flight_no_2, flight_no_3, flight_no_4, fuel_remaining_total,
        fuel_tank_qty_left_wing, fuel_tank_qty_right_wing, fuel_tank_qty_center
    from dwh_staging.fact_flight_millisecond;

    -- updating fact_leg fuel info
    update dwh_presentation.fact_leg u
    set fuel_qty_on_flight_record_start = b.fuel_qty_on_flight_start_record,
        fuel_qty_on_flight_record_finish = b.fuel_qty_on_flight_finish_record,
        aircraft_weight_pounds_start_record = b.aircraft_weight_pounds_start_record,
        aircraft_weight_pounds_finish_record = b.aircraft_weight_pounds_finish_record,
        aircraft_weight_tons_start_record = b.aircraft_weight_tons_start_record,
        aircraft_weight_tons_finish_record = b.aircraft_weight_tons_finish_record,
        aircraft_weight_without_fuel_tons_start_record = b.aircraft_weight_without_fuel_tons_start_record,
        aircraft_weight_without_fuel_tons_finish_record = b.aircraft_weight_without_fuel_tons_finish_record
    from dwh_staging.bridge_fact_flight_millisecond_to_fact_leg b
    where b.md5_leg = u.md5_leg;

end
$$;