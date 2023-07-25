do $dwh_staging$
begin
    drop table dwh_staging.fact_takeoff_landing;
    create table dwh_staging.fact_takeoff_landing
    (
        id serial,
        remark varchar,
        id_source integer,
        id_fact_leg integer,
        id_takeoff_landing_kind integer,
        id_date integer,
        id_time integer,
        id_board integer,
        id_flight integer,
        id_airport integer,
        id_captain integer,
        id_runway integer,
        datetime timestamp,
        skytrac_approx_position varchar,
        track numeric,
        latitude numeric,
        longitude numeric,
        altitude_ft numeric,
        speed_kts numeric,
        distance_m numeric
    );

    alter table dwh_staging.fact_takeoff_landing owner to dwh;
end;
$dwh_staging$;


do $dwh_presentation$
begin
    drop table dwh_presentation.fact_takeoff_landing;
    create table dwh_presentation.fact_takeoff_landing
    (
        id serial,
        id_fact_leg integer,
        id_takeoff_landing_kind integer,
        id_date integer,
        id_time integer,
        id_board integer,
        id_flight integer,
        id_airport integer,
        id_captain integer,
        id_runway integer,
        datetime timestamp,
        skytrac_approx_position varchar,
        track numeric,
        latitude numeric,
        longitude numeric,
        altitude_ft numeric,
        speed_kts numeric,
        distance_m numeric
    );

    alter table dwh_presentation.fact_takeoff_landing owner to dwh;
end;
$dwh_presentation$;
