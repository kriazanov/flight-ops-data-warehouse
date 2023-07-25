create or replace function dwh_staging.sf_dim_crew_task_division(p_code varchar, p_name varchar)
returns integer
language plpgsql
as $$
declare
    v_id integer;
begin
    insert into dwh_presentation.dim_crew_task_division(md5, code, name)
    select md5(p_code || p_name), p_code, p_name
    on conflict (md5) do update
    set md5 = excluded.md5,
        code = excluded.code,
        name = excluded.name
    returning id into v_id;

    return v_id;
end
$$