drop table dwh_presentation.dim_time;
create table dwh_presentation.dim_time
(
	id integer,
	time text,
	time_am_pm text,
	meridiem text,
	time_meridiem text,
	hours text,
	minutes text,
	unique(id)
);

alter table dwh_presentation.dim_time owner to dwh;

