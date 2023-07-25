drop table dwh_staging.raw_sap_credit_debit_bill_soap_response;
create table dwh_staging.raw_sap_credit_debit_bill_soap_response
(
    response_content xml,
    date_created     timestamp,
    date_processed   timestamp,
    debit_or_credit  varchar
);
