do $dwh_staging$
begin
    drop table dwh_staging.bridge_fact_flight_millisecond_to_fact_leg;
    create table dwh_staging.bridge_fact_flight_millisecond_to_fact_leg
    (
        id_board integer,
        end_record timestamp,
        md5_leg varchar,
        id_flight integer,
        id_origin integer,
        id_destination integer,
        fuel_qty_on_flight_start_record float,
        fuel_qty_on_flight_finish_record float,
        aircraft_weight_pounds_start_record float,
        aircraft_weight_pounds_finish_record float,
        aircraft_weight_tons_start_record float,
        aircraft_weight_tons_finish_record float,
        aircraft_weight_without_fuel_tons_start_record float,
        aircraft_weight_without_fuel_tons_finish_record float
    );

    alter table dwh_staging.bridge_fact_flight_millisecond_to_fact_leg owner to dwh;
end;
$dwh_staging$;


do $dwh_presentation$
begin
    drop table dwh_presentation.bridge_fact_flight_millisecond_to_fact_leg;
    create table dwh_presentation.bridge_fact_flight_millisecond_to_fact_leg
    (
        id_board integer,
        end_record timestamp,
        md5_leg varchar,
        id_flight integer,
        id_origin integer,
        id_destination integer,
        fuel_qty_on_flight_start_record float,
        fuel_qty_on_flight_finish_record float,
        aircraft_weight_pounds_start_record float,
        aircraft_weight_pounds_finish_record float,
        aircraft_weight_tons_start_record float,
        aircraft_weight_tons_finish_record float,
        aircraft_weight_without_fuel_tons_start_record float,
        aircraft_weight_without_fuel_tons_finish_record float
    );

    alter table dwh_presentation.bridge_fact_flight_millisecond_to_fact_leg owner to dwh;

end;
$dwh_presentation$;
