drop table dwh_presentation.dim_flight;
create table dwh_presentation.dim_flight
(
    id serial,
    md5 varchar,
    carrier_code varchar,
    subcarrier_code varchar,
	flight_no varchar,
	litera varchar,
	cshare varchar,
	conn_type varchar,
	route varchar,
	flt_kind varchar,
	transp_type varchar,
	remark varchar,
    status varchar,
    stc varchar,
    unique (md5)
);

alter table dwh_presentation.dim_flight owner to dwh;

