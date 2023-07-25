# Aircraft Availability Report (Dayflight Condition)

> **Customer:** Department of Economy
>  
> **Source System:** AMOS Helicopters
> 
> **Fact type:** Snapshot
> 
> **ETL DAG:** `dwh_sap_credit_debit_bill`
> 
> **DW/BI Staging table:** `report_dayflight_condition_helicopters`
> 
> **DW/BI Presentation table:** `report_dayflight_condition_helicopters`

## Description
This report maintains information about helicopter boards availability, divided by flight number.
For proper processing, it needs to have additional dictionaries*:

* `dict_heli_dayflight_point` contains business entity "direction"
* `dict_heli_dayflight_aircraft_type` contains information about custom "groups of aircrafts"

> *Additional dictionaries are not contained in any system of the aircompany.

## Delivery
1. Connect to AMOS Database via PostgreSQL Hook.
2. Execute SQL query to get data and build dataset.
3. Deliver dataset to DWH staging area.
4. Call stored procedure `sf_prepare_heli_dayflight_condition` to process data and publish result to presentation layer.
