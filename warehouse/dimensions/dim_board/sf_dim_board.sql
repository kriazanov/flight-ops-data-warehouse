create or replace function dwh_staging.sf_dim_board(
    p_is_update boolean,
	p_tail_no varchar,
	p_ac_type_code varchar,
	p_ac_type_name varchar)
returns integer
language plpgsql
as $$
declare
    v_id integer;
begin
    if p_is_update = true
    then
        insert into dwh_presentation.dim_board(tail_no, ac_type_code, ac_type_name)
        select distinct
            p_tail_no,
            case
                when p_ac_type_code in ('VQ-BLF', 'VQ-BLL', 'VQ-BMD', 'VQ-BLG', 'VQ-BLI')
                then 'AT72'
            end as ac_type_code,
            p_ac_type_name
        on conflict (tail_no) do update
        set ac_type_code = excluded.ac_type_code,
            ac_type_name = excluded.ac_type_name
        returning id into v_id;
    else
        insert into dwh_presentation.dim_board(tail_no, ac_type_code, ac_type_name)
        select distinct
            p_tail_no,
            case
                when p_ac_type_code in ('VQ-BLF', 'VQ-BLL', 'VQ-BMD', 'VQ-BLG', 'VQ-BLI')
                then 'AT72'
            end as ac_type_code,
            p_ac_type_name
        on conflict (tail_no) do nothing;

        select id into v_id
        from dwh_presentation.dim_board
        where 1=1
        and tail_no = p_tail_no;
    end if;

    return v_id;
end;
$$;