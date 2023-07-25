drop table dwh_staging.report_helicopters_fleet;
create table dwh_staging.report_helicopters_fleet
(
    ac_registr varchar,
    ac_typ varchar,
    serialno varchar,
    manufacturer varchar,
    asset_owner varchar,
    owner varchar,
    fa_costcenter varchar,
    mfg_date date,
    del_date date,
    cert_issue_date date,
    cert_exp_date_from date,
    cert_exp_date_until date,
    slg_number varchar,
    homebase varchar,
    fin_info varchar,
    status varchar,
    cycles float,
    h float,
    ch float,
    days float,
    ppr float,
    cp float,
    chp float,
    non_managed varchar(1)
);

drop table dwh_presentation.report_helicopters_fleet cascade ;

create table dwh_presentation.report_helicopters_fleet
(
    ac_registr varchar,
    ac_typ varchar,
    serialno varchar,
    manufacturer varchar,
    asset_owner varchar,
    owner varchar,
    fa_costcenter varchar,
    mfg_date date,
    del_date date,
    cert_issue_date date,
    cert_exp_date_from date,
    cert_exp_date_until date,
    slg_number varchar,
    homebase varchar,
    fin_info varchar,
    status varchar,
    cycles float,
    h float,
    ch float,
    days float,
    ppr float,
    cp float,
    chp float,
    non_managed varchar(1)
);