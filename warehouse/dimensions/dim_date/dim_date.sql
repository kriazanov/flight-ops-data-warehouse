drop table dwh_presentation.dim_date;
create table dwh_presentation.dim_date
(
	id integer,
	date date,
	date_str text,
	year text,
	year_short text,
	half_year_code text,
	half_year_name text,
	quarter_no float,
	quarter_code text,
	quarter_name text,
	month_number text,
	month_uppercase text,
	month_capitalized text,
	month_upper_short text,
	month_capitalized_short text,
	week_month text,
	week_year text,
	day_week_uppercase text,
	day_week text,
	day_short_uppercase text,
	day_short text,
	day_year text,
	day_month text,
	day_week_number text,
	unique(id)
);

alter table dwh_presentation.dim_date owner to dwh;

