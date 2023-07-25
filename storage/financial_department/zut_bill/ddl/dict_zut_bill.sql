drop table dwh_staging.dict_zut_bill;

create table dwh_staging.dict_zut_bill(
    tag varchar,
    t_key varchar,
    t_val varchar
);

alter table dwh_staging.dict_zut_bill owner to dwh;