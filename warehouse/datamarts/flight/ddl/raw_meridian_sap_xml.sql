drop table dwh_staging.raw_meridian_sap_xml;
create table dwh_staging.raw_meridian_sap_xml
(
	id serial,
	file_name varchar,
	file_content xml,
	date_created timestamp,
	date_processed timestamp,
	unique(file_name)
);

alter table dwh_staging.raw_meridian_sap_xml owner to dwh;

