create or replace function dwh_staging.sf_dim_flight(
    p_carrier_code varchar,
    p_subcarrier_code varchar,
	p_flight_no varchar,
	p_litera varchar,
	p_cshare varchar,
	p_conn_type varchar,
	p_route varchar,
	p_flt_kind varchar,
	p_transp_type varchar,
	p_remark varchar,
    p_status varchar,
    p_stc varchar)
returns integer
language plpgsql
as $$
declare
    v_id integer;
begin
    insert into dwh_presentation.dim_flight(md5, carrier_code, subcarrier_code, flight_no, litera, cshare, conn_type, route, flt_kind, transp_type, remark, status, stc)
    select distinct
        md5(
            coalesce(p_carrier_code, '') ||
            coalesce(p_subcarrier_code, '') ||
            coalesce(p_flight_no, '') ||
            coalesce(p_litera, '') ||
            coalesce(p_cshare, '') ||
            coalesce(p_conn_type, '') ||
            coalesce(p_route, '') ||
            coalesce(p_flt_kind, '') ||
            coalesce(p_transp_type, '') ||
            coalesce(p_remark, '') ||
            coalesce(p_status, '') ||
            coalesce(p_stc, '')) as md5,
        p_carrier_code, p_subcarrier_code, p_flight_no, p_litera, p_cshare, p_conn_type, p_route, p_flt_kind, p_transp_type, p_remark, p_status, p_stc
    on conflict (md5) do update
        set md5 = excluded.md5,
            carrier_code = excluded.carrier_code,
            subcarrier_code = excluded.subcarrier_code,
            flight_no = excluded.flight_no,
            litera = excluded.litera,
            cshare = excluded.cshare,
            conn_type = excluded.conn_type,
            route = excluded.route,
            flt_kind = excluded.flt_kind,
            transp_type = excluded.transp_type,
            status = excluded.status,
            remark = excluded.remark
    returning id into v_id;

    return v_id;
end;
$$;