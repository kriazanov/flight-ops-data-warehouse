-- staging
drop table dwh_presentation.dim_board;
create table dwh_presentation.dim_board
(
    id serial,
	tail_no varchar,
	ac_type_code varchar,
	ac_type_name varchar,
    unique(tail_no)
);

alter table dwh_presentation.dim_board owner to dwh;

