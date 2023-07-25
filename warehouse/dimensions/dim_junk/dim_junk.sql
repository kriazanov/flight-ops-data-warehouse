drop table dwh_presentation.dim_junk;
create table dwh_presentation.dim_junk
(
    id serial,
    takeoff_landing_kind varchar
);

alter table dwh_presentation.dim_junk owner to dwh;

