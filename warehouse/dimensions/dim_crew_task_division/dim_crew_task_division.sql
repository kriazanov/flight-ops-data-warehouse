drop table dwh_presentation.dim_crew_task_division;
create table dwh_presentation.dim_crew_task_division(
    id serial,
    md5 varchar unique,
    code varchar,
    name varchar
);
alter table dwh_presentation.dim_crew_task_division owner to dwh;
