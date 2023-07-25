do
$dwh_staging$
begin

    drop table dwh_staging.report_salary_piecework;

    create table dwh_staging.report_salary_piecework
    (
        task_reference varchar,
        descno_i varchar,
        ext_workorderno varchar,
        event_perfno_i varchar,
        created_date varchar,
        issue_date varchar,
        issue_time varchar,
        closing_date varchar,
        closing_time varchar,
        description varchar,
        special varchar,
        special_vne varchar,
        prj_description varchar,
        issue_station2 varchar,
        ac_registr varchar,
        ac_type varchar,
        owner_name varchar,
        ws_count varchar,
        total varchar,
        b13 varchar,
        b2 varchar,
        lab_d varchar,
        lab_aiero varchar,
        tr varchar,
        pp varchar,
        ogm varchar,
        fact_tr varchar,
        at_code varchar,
        task_ref_diff varchar,
        st_d varchar,
        end_d varchar,
        meta_key varchar,
        period varchar,
        issue_sign varchar,
        closing_sign varchar,
        issue_sign_short varchar,
        mech_sign varchar,
        text_task varchar,
        rem_text varchar,
        wpno varchar,
        rlmt varchar,
        tip_to varchar,
        state varchar,
        is_added bool default false
    );

    alter table dwh_staging.report_salary_piecework owner to dwh;

end;
$dwh_staging$;

do
$dwh_presentation$
begin

    drop table dwh_presentation.report_salary_piecework cascade;

    create table dwh_presentation.report_salary_piecework
    (
        task_reference varchar,
        descno_i varchar,
        ext_workorderno varchar,
        event_perfno_iint,
        created_date date,
        issue_date date,
        issue_time time,
        closing_date date,
        closing_time time,
        description varchar,
        special varchar,
        special_vne varchar,
        prj_description varchar,
        issue_station2 varchar,
        ac_registr varchar,
        ac_type varchar,
        owner_name varchar,
        ws_count float,
        total float,
        b13 float,
        b2 float,
        lab_d float,
        lab_aiero float,
        tr float,
        pp float,
        ogm float,
        fact_tr varchar,
        at_code varchar,
        task_ref_diff varchar,
        st_d date,
        end_d date,
        meta_key varchar,
        period int,
        issue_sign varchar,
        closing_sign varchar,
        issue_sign_short varchar,
        mech_sign varchar,
        text_task varchar,
        rem_text varchar,
        wpno varchar,
        rlmt varchar,
        tip_to varchar,
        state varchar,
        constraint unq_key unique (descno_i, event_perfno_i, meta_key));

    alter table dwh_presentation.report_salary_piecework owner to dwh;
    grant select on dwh_presentation.report_salary_piecework to readonly;
end;
$dwh_presentation$;

