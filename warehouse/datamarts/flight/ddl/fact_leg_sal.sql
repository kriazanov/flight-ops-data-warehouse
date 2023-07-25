drop table dwh_staging.fact_leg_sal;
create table dwh_staging.fact_leg_sal
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
    subcarrier_code varchar,
    flight_no varchar,
    flight_date timestamp,
    litera varchar,
    cshare varchar,
    conn_type varchar,
    flight_tail_no varchar,
    route varchar,
    flt_kind varchar,
    transp_type varchar,
    remark varchar,
    actual_date timestamp,

    --leg
    leg_tail_no varchar,
    origin_iata varchar,
    destination_iata varchar,
    departure_plan_local timestamp,
    departure_plan_utc timestamp,
    arrival_plan_local timestamp,
    arrival_plan_utc timestamp,
    departure_fact_local timestamp,
    departure_fact_utc timestamp,
    arrival_fact_local timestamp,
    arrival_fact_utc timestamp,
    power_on timestamp,
    power_off timestamp,
    on_surface varchar,
    f_nr integer,
    c_nr integer,
    y_nr integer,
    child_nr integer,
    infant_nr integer,
    crg_load integer,
    luggage_load integer,
    hand_luggage integer,
    mail_load integer,
    cabin_baggage integer,
    emptying integer,
    fueling integer,
    dep_remnant integer,
    arr_remnant integer,
    f_seats_occupied integer,
    c_seats_occupied integer,
    y_seats_occupied integer,
    payload integer,
    f_child integer,
    c_child integer,
    y_child integer,
    f_infant integer,
    c_infant integer,
    y_infant integer,
    distance integer,
    equipped_empty_kg integer,
    flight_task varchar
);

alter table dwh_staging.fact_leg_sal owner to dwh;
grant select on dwh_staging.fact_leg_sal to readonly;