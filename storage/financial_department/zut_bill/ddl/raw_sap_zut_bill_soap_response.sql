drop table dwh_staging.raw_sap_zut_bill_soap_response;
create table dwh_staging.raw_sap_zut_bill_soap_response(
    response_content xml,
	date_created timestamp,
	date_processed timestamp
);
alter table dwh_staging.raw_sap_zut_bill_soap_response owner to dwh;