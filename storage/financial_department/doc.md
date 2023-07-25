# Budget and Financial Control Datamart

> **Customer:** Financial Department of Budget and Financial Control
>  
> **Source System:** SAP

## Datamart Description

The Budget and Financial Control Department wants to generate their reports from the SAP system automatically. All report forms have already been designed, but currently, the data needs to be extracted manually.

The required data consists of three datasets, each containing required transactions from the SAP system. These datasets will be described in the table at the end of this documentation page.

## Delivery
In general, all datasets can be retrieved in a similar way. 
The SAP system has a SOAP server, which requires a SOAP request and returns a SOAP response.

### Extract
To extract the required data, it needs to programmatically build the request body in the Airflow DAG. 
Once the request is sent, it takes a few seconds to get an XML response. All data should be placed in `raw_` tables as is, in an XML field with the entire XML file content.

### Transform
Transform procedures run on the database side, so they should be executed directly from the database. 
The purpose of these procedures is to extract XML and place all the extracted data into `report_` tables right in the staging area.

### Load
The final step is to load the extracted and processed data fields to the presentation layer into the identically named `report_` tables.

## List of Datasets

|DATASET          |ETL DAG                  |
|-----------------|-------------------------|
|credit_debit_bill|dwh_sap_credit_debit_bill|
|zut_bill         |dwh_sap_zut_bill         |
|zutvu            |dwh_sap_zutvu            |
