drop table dwh_presentation.dim_crew_task_member;
create table dwh_presentation.dim_crew_task_member(
    id serial,
    md5 varchar unique,
    tab_no varchar,
    full_name varchar,
    role varchar
);
alter table dwh_presentation.dim_crew_task_member owner to dwh;
