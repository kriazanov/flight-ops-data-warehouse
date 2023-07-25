drop table dwh_staging.raw_skytrac_mails;
create table dwh_staging.raw_skytrac_mails
(
	id serial,
	mail_mailbox_id varchar,
	mail_date varchar,
	mail_from varchar,
	mail_to varchar,
	mail_subject varchar,
	mail_body text,
	raw_board varchar,
	raw_takeoff_landing_kind varchar,
	raw_datetime varchar,
	raw_lat varchar,
	raw_long varchar,
	raw_skytrac_approx_position varchar,
	raw_alt varchar,
	raw_speed varchar,
	raw_track varchar,
	decoded_iso_datetime_utc varchar,
	decoded_lat_newest_format varchar,
	decoded_lat_new_format varchar,
	decoded_lat_historic_format varchar,
	decoded_long_newest_format varchar,
	decoded_long_new_format varchar,
	decoded_long_historic_format varchar,
	decoded_alt_value varchar,
	decoded_alt_mu varchar,
	decoded_speed_value varchar,
	decoded_speed_mu varchar,
	date_created timestamp,
	date_processed timestamp
);

alter table dwh_staging.raw_skytrac_mails owner to dwh;
