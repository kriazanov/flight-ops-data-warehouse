create or replace function dwh_staging.sf_perform_report_salary_piecework() returns void
    language plpgsql
as
$$
begin

    insert into dwh_presentation.report_salary_piecework

    select distinct
        task_reference,
        descno_i,
        ext_workorderno,
        event_perfno_i::int as event_perfno_i,
        to_date(created_date, 'DD.MM.YYYY') as created_date,
        to_date(issue_date, 'DD.MM.YYYY') as issue_date,
        to_timestamp(issue_time, 'HH24:MI') as issue_time,
        to_date(closing_date, 'DD.MM.YYYY') as closing_date,
        to_timestamp(closing_time, 'HH24:MI') as closing_time,
        description,
        special,
        special_vne,
        prj_description,
        issue_station2,
        ac_registr,
        ac_type,
        owner_name,
        ws_count::float as ws_count,
        total::float as total,
        b13::float as b12,
        b2::float as b2,
        lab_d::float as lab_d,
        lab_aiero::float as lab_aiero,
        tr::float as tr,
        pp::float as pp,
        ogm::float as ogm,
        fact_tr,
        at_code,
        task_ref_diff,
        to_date(st_d, 'DD.MM.YYYY') as st_d,
        to_date(end_d, 'DD.MM.YYYY') as end_d,
        coalesce(meta_key, ''),
        period::int as period,
        issue_sign,
        closing_sign,
        issue_sign_short,
        mech_sign,
        text_task,
        rem_text,
        coalesce(wpno, '') as wpno,
        rlmt,
        coalesce(tip_to, '') as tip_to,
        state

    from dwh_staging.report_salary_piecework
    on conflict (descno_i, event_perfno_i, meta_key, wpno, tip_to)
    do update set
        descno_i = coalesce(excluded.descno_i, ''),
        event_perfno_i = excluded.event_perfno_i,
        meta_key = coalesce(excluded.meta_key, ''),
        wpno = coalesce(excluded.wpno, ''),
        tip_to = coalesce(excluded.tip_to, ''),
        task_reference = excluded.task_reference,
        ext_workorderno = excluded.ext_workorderno,
        created_date = to_date(to_char(excluded.created_date, 'DD.MM.YYYY'), 'DD.MM.YYYY'),
        issue_date = to_date(to_char(excluded.issue_date, 'DD.MM.YYYY'), 'DD.MM.YYYY'),
        issue_time = to_timestamp(excluded.issue_time::varchar, 'HH24:MI'),
        closing_date = to_date(to_char(excluded.closing_date, 'DD.MM.YYYY'), 'DD.MM.YYYY'),
        closing_time = to_timestamp(excluded.closing_time::varchar, 'HH24:MI'),
        description = excluded.description,
        special = excluded.special,
        special_vne = excluded.special_vne,
        prj_description = excluded.prj_description,
        issue_station2 = excluded.issue_station2,
        ac_registr = excluded.ac_registr,
        ac_type = excluded.ac_type,
        owner_name = excluded.owner_name,
        ws_count = excluded.ws_count::float,
        total = excluded.total::float,
        b13 = excluded.b13::float,
        b2 = excluded.b2::float,
        lab_d = excluded.lab_d::float,
        lab_aiero = excluded.lab_aiero::float,
        tr = excluded.tr::float,
        pp = excluded.pp::float,
        ogm = excluded.ogm::float,
        fact_tr = excluded.fact_tr,
        at_code = excluded.at_code,
        task_ref_diff = excluded.task_ref_diff,
        st_d = to_date(to_char(excluded.st_d, 'DD.MM.YYYY'), 'DD.MM.YYYY'),
        end_d = to_date(to_char(excluded.end_d, 'DD.MM.YYYY'), 'DD.MM.YYYY'),
        period = excluded.period::int,
        issue_sign = excluded.issue_sign,
        closing_sign = excluded.closing_sign,
        issue_sign_short = excluded.issue_sign_short,
        mech_sign = excluded.mech_sign,
        text_task = excluded.text_task,
        rem_text = excluded.rem_text,
        rlmt = excluded.rlmt,
        state = excluded.state;
    
    truncate dwh_staging.report_salary_piecework;
end
$$;

alter function dwh_staging.sf_perform_report_salary_piecework() owner to dwh;
