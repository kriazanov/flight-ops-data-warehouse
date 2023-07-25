drop table dwh_staging.raw_staffonly_files_decrypted;
create table dwh_staging.raw_staffonly_files_decrypted(
    tail_no varchar(10),
    end_record timestamp,
    param_no integer,
    param_key timestamp,
    param_value float
);

alter table dwh_staging.raw_staffonly_files_decrypted owner to dwh;
