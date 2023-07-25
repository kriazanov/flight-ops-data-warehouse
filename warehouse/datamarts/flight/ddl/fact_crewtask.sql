do $dwh_staging$
begin
    drop table dwh_staging.fact_crewtask;
    create table dwh_staging.fact_crewtask
    (
        md5_flight varchar,
        id_date integer,
        id_time integer,
        id_task_date integer,
        id_flight integer,
        id_board integer,
        id_origin integer,
        id_destination integer,
        id_crew_task_member integer,
        id_crew_task_division integer,
        id_crew_task_member_division integer,
        crew_task_no varchar
    );

    alter table dwh_staging.fact_crewtask owner to dwh;
    grant select on dwh_presentation.fact_leg to readonly;
end;
$dwh_staging$;

do $dwh_presentation$
begin
    drop table dwh_presentation.fact_crewtask;
    create table dwh_presentation.fact_crewtask
    (
        id serial,
        md5_flight varchar,
        id_date integer,
        id_time integer,
        id_task_date integer,
        id_flight integer,
        id_board integer,
        id_origin integer,
        id_destination integer,
        id_crew_task_member integer,
        id_crew_task_division integer,
        id_crew_task_member_division integer,
        crew_task_no varchar
    );

    alter table dwh_presentation.fact_crewtask owner to dwh;
    grant select on dwh_presentation.fact_leg to readonly;

end;
$dwh_presentation$;