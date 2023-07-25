drop table dwh_presentation.dim_station;

create table dwh_presentation.dim_station
(
    id serial,
    code varchar,
	station_iata varchar,
	station_icao varchar,
	station_name_ru varchar,
	station_name_en varchar,
	unique(code)
);

alter table dwh_presentation.dim_station owner to dwh;
grant select on dwh_presentation.dim_station to readonly;