create or replace function dwh_staging.sf_dim_crew_task_member(p_tab_no varchar, p_full_name varchar, p_role varchar)
returns integer
language plpgsql
as $$
declare
    v_id integer;
begin
    insert into dwh_presentation.dim_crew_task_member(md5, tab_no, full_name, role)
    select md5(p_tab_no || p_full_name || p_role), p_tab_no, p_full_name, p_role
    on conflict (md5) do update
    set md5 = excluded.md5,
        tab_no = excluded.tab_no,
        full_name = excluded.full_name,
        role = excluded.role
    returning id into v_id;

    return v_id;
end
$$