# Debit & Credit Report

> **Customer:** Financial Department of Budget and Financial Control
>  
> **Source System:** SAP
> 
> **Fact type:** Snapshot
> 
> **ETL DAG:** `dwh_sap_credit_debit_bill`
> 
> **DW/BI RAW table:** `raw_sap_zut_bill_soap_response`
> 
> **DW/BI Presentation table:** `report_debit_credit_bill`

## Description
Contains debit/credit dept financial information.
Target table not accumulative, it refreshes every ETL iteration, a whole data will be purged.
All required data comes from SAP web server and consists all required information in XML response.
ETL wipes all existing data and populate the table with a new one.

## Delivery
1. Connect to SAP data source web server.
2. Retrieving XML response.
3. Dataset delivery to DWH staging area.
4. Call stored procedure `sf_prepare_sap_debit_credit_bill` from database directly.

> Stored procedure extract staged XML data and publicates data to Presentation Area.
> Once data was published to presentation layer, it is able to call Ad Hoc queries from any analytic tools.
