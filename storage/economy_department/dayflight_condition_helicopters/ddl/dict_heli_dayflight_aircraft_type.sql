drop table dwh_staging.dict_heli_dayflight_aircraft_type;

create table dwh_staging.dict_heli_dayflight_aircraft_type(
    id serial,
    ac_type varchar,
    ac_type_group varchar);

alter table dwh_staging.dict_heli_dayflight_aircraft_type owner to dwh;