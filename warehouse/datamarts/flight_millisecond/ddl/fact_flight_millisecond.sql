do $dwh_staging$
begin
    drop table dwh_staging.fact_flight_millisecond;
    create table dwh_staging.fact_flight_millisecond
    (
        id_flight integer,
        id_board integer,
        id_date integer,
        id_time integer,
        id_flight_parameter integer,
        id_origin integer,
        id_destination integer,
        end_record timestamp,
        param_key timestamp,
        param_value float,
        flight_no_1 float,
        flight_no_2 float,
        flight_no_3 float,
        flight_no_4 float,
        pilot_input_flight_no varchar,
        fuel_remaining_total float,
        fuel_tank_qty_left_wing float,
        fuel_tank_qty_right_wing float,
        fuel_tank_qty_center float,
        aircraft_weight_pounds float,
        aircraft_weight_tons float,
        aircraft_weight_without_fuel_tons float
    );

    alter table dwh_staging.fact_flight_millisecond owner to dwh;

end;
$dwh_staging$;


do $dwh_presentation$
begin
    drop table dwh_presentation.fact_flight_millisecond;
    create table dwh_presentation.fact_flight_millisecond
    (
        id_flight integer,
        id_board integer,
        id_date integer,
        id_time integer,
        id_flight_parameter integer,
        id_origin integer,
        id_destination integer,
        end_record timestamp,
        param_key timestamp,
        param_value float,
        pilot_input_flight_no varchar,
        flight_no_1 float,
        flight_no_2 float,
        flight_no_3 float,
        flight_no_4 float,
        fuel_remaining_total float,
        fuel_tank_qty_left_wing float,
        fuel_tank_qty_right_wing float,
        fuel_tank_qty_center float,
        aircraft_weight_pounds float,
        aircraft_weight_tons float,
        aircraft_weight_without_fuel_tons float
    );

    alter table dwh_presentation.fact_flight_millisecond owner to dwh;
end;
$dwh_presentation$;
