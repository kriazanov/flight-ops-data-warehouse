# ZUTVU Transaction Report

> **Customer:** Financial Department of Budget and Financial Control
>  
> **Source System:** SAP 
> 
> **Fact type:** Snapshot
> 
> **ETL DAG:** `dwh_sap_zutvu`
> 
> **DW/BI RAW table:** `raw_sap_zutvu_soap_response`
> 
> **DW/BI Presentation table:** `report_zutvu`

## Description
The presentation dataset table contains information required for financial helicopter segment management purposes. It is not accumulative; with every ETL iteration, all data will be wiped and populated with new data.

The report contains a large number of fields, so it is necessary to have full control of the data flow. There are two tables `report_zutvu` in both staging and presentation areas, but field names are quite different.

* All attribute names in the **staging area** have the same names as in the delivered XML. We keep the exact SAP names for debugging purposes between DW/BI team and SAP developers team.
* All attribute names in the **presentation area** have friendly names for easy access to data.
* All attribute names in the **view** have maximum friendly Cyrillic names for our colleagues from the Budget and Financial Control Department.

## ETL Steps
1. Connect to SAP data source web server.
2. Retrieve XML response.
3. Deliver the dataset to the DWH staging area.
4. Call the stored procedure `sf_prepare_sap_zutvu` directly from the database.
