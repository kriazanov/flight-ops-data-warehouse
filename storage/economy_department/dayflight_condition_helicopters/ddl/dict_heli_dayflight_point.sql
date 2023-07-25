drop table dwh_staging.dict_heli_dayflight_point;

create table dwh_staging.dict_heli_dayflight_point(
    id serial,
    area_name varchar,
    direction_code varchar);

alter table dwh_staging.dict_heli_dayflight_point owner to dwh;