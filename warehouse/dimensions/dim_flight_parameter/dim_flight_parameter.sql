drop table dwh_presentation.dim_flight_parameter;
create table dwh_presentation.dim_flight_parameter
(
	id serial,
	tail_no varchar(10),
	param_id integer,
	param_code varchar(200),
	param_name varchar(200),
	measure_unit varchar(200)
);

alter table dwh_presentation.dim_flight_parameter owner to dwh;

