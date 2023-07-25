# ZUT BILL Transaction Report

> **Customer:** Financial Department of Budget and Financial Control
>  
> **Source System:** SAP 
> 
> **Fact type:** Snapshot
> 
> **ETL DAG:** `dwh_sap_zut_bill`
> 
> **DW/BI RAW table:** `raw_sap_zut_bill_soap_response`
> 
> **DW/BI Presentation table:** `report_zut_bill`

## Description
The presentation dataset table contains information required for financial helicopter segment management purposes. 
It is not accumulative; with every ETL iteration, all data will be truncated and populated with new data.

To properly process all data, it requires an additional **measure unit dictionary** from SAP system. 
The dictionary was downloaded manually from SAP system and uploaded to DWH table `dict_sap_measure_unit` in staging area. 
The dictionary table contains data as-is.

## Delivery
1. Connect to SAP data source web server.
2. Retrieve XML response.
3. Deliver dataset to DWH staging area.
4. Call stored procedure `sf_transform_sap_zut_bill` directly from database.
