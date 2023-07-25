create or replace function dwh_staging.sf_dim_time(p_id integer)
returns integer
language plpgsql
as $$
begin
    insert into dwh_presentation.dim_time(id, time, time_am_pm, meridiem, time_meridiem, hours, minutes)
    select
        t.id as id,
        to_char(t.time, 'HH24:MI') AS time,
        to_char(t.time, 'HH:MI') AS time_am_pm,
        to_char(t.time, 'AM') AS meridiem,
        to_char(t.time, 'HH:MI AM') AS time_meridiem,
        to_char(t.time, 'HH24') AS hours,
        to_char(t.time, 'MI') AS minutes
    from(
        select distinct p_id as id,
            case length(p_id::varchar)
                when 0 then '0000'
                when 1 then '000'
                when 2 then '00'
                when 3 then '0'
            else '' end || p_id::varchar as time_str,
            to_timestamp(case length(p_id::varchar)
                when 0 then '0000'
                when 1 then '000'
                when 2 then '00'
                when 3 then '0'
            else '' end || p_id::varchar, 'HH24MI') as time) t
    on conflict(id) do nothing;
    return p_id;
end;
$$;