create table dwh_staging.raw_sap_zutvu_soap_response(
    response_content xml,
    date_created timestamp,
    date_processed timestamp
);

alter table dwh_staging.raw_sap_zutvu_soap_response owner to dwh;
