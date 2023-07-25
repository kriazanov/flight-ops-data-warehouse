drop table dwh_presentation.dim_runway;
create table dwh_presentation.dim_runway
(
    id serial,
	airport_iata varchar,
	airport varchar,
	runway_no varchar,
	runway_course_true_brg numeric,
	runway_course_mag_brg numeric,
	runway_length varchar,
	runway_end_lat numeric,
	runway_end_long numeric
);

alter table dwh_presentation.dim_runway owner to dwh;

