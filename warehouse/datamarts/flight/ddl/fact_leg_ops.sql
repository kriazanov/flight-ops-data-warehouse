drop table dwh_staging.fact_leg_ops;
create table dwh_staging.fact_leg_ops
(
    --id's
    md5_flight varchar,
    md5_leg varchar,
    id_date int,
    id_time int,
    id_flight int,
    id_board int,
    id_origin int,
    id_destination int,

    --source
    source_code varchar,
    source varchar,

    --flight
    carrier_code varchar,
    external_flight_id varchar,
    flight_no varchar,
    flight_date timestamp,
    litera varchar,
    cshare varchar,
    actual_date timestamp,

    --leg
    leg_tail_no varchar,
    ac_type_code varchar,
    ac_type_name varchar,
    origin_iata varchar,
    origin_icao varchar,
    origin_name_ru varchar,
    origin_name_en varchar,
    destination_iata varchar,
    destination_icao varchar,
    destination_name_ru varchar,
    destination_name_en varchar,
    delay_codes varchar,
    status varchar,
    stc varchar,
    departure_plan_local timestamp,
    departure_plan_utc timestamp,
    arrival_plan_local timestamp,
    arrival_plan_utc timestamp,
    departure_est_local timestamp,
    departure_est_utc timestamp,
    arrival_est_local timestamp,
    arrival_est_utc timestamp,
    departure_fact_local timestamp,
    departure_fact_utc timestamp,
    arrival_fact_local timestamp,
    arrival_fact_utc timestamp,
    pax_transit integer,
    f_nr integer,
    c_nr integer,
    y_nr integer,
    f_seats_total integer,
    c_seats_total integer,
    y_seats_total integer
);

alter table dwh_staging.fact_leg_ops owner to dwh;
grant select on dwh_staging.fact_leg_ops to readonly;
