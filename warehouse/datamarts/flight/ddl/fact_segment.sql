do $dwh_staging$
begin
    drop table dwh_staging.fact_segment;
    create table dwh_staging.fact_segment
    (
        md5_flight varchar,
        id_date integer,
        id_time integer,
        id_flight integer,
        id_board integer,
        id_city_from integer,
        id_city_to integer,
        id_origin integer,
        id_destination integer,
        id_date_actual integer,
        f_adult integer,
        f_child integer,
        f_infant integer,
        c_adult integer,
        c_child integer,
        c_infant integer,
        y_adult integer,
        y_child integer,
        y_infant integer,
        luggage_kg float,
        ex_luggage_kg float,
        cargo_kg float,
        mail_kg float,
        flight_task varchar
    );

    alter table dwh_staging.fact_segment owner to dwh;
    grant select on dwh_presentation.fact_leg to readonly;
end;
$dwh_staging$;


do $dwh_presentation$
begin
    drop table dwh_presentation.fact_segment;
    create table dwh_presentation.fact_segment
    (
        id serial,
        md5_flight varchar,
        id_date integer,
        id_time integer,
        id_flight integer,
        id_board integer,
        id_city_from integer,
        id_city_to integer,
        id_origin integer,
        id_destination integer,
        id_date_actual integer,
        f_adult integer,
        f_child integer,
        f_infant integer,
        c_adult integer,
        c_child integer,
        c_infant integer,
        y_adult integer,
        y_child integer,
        y_infant integer,
        luggage_kg float,
        ex_luggage_kg float,
        cargo_kg float,
        mail_kg float,
        flight_task varchar
    );

    alter table dwh_presentation.fact_segment owner to dwh;
    grant select on dwh_presentation.fact_leg to readonly;

end;
$dwh_presentation$;


