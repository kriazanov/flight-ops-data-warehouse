drop table dwh_staging.raw_meridian_board_xml;
create table dwh_staging.raw_meridian_board_xml
(
	id serial,
	file_content xml,
	date_created timestamp,
	date_processed timestamp
);

alter table dwh_staging.raw_meridian_board_xml owner to dwh;

