# Engineering Workers KPI Monitoring

> **Customer:** Department Of Strategy (Engineering Branch)
>  
> **Source System:** AMOS Helicopters
> 
> **Fact type:** Snapshot

## Description
This datamart contains a list of datasets, each representing separate KPIs. 
All data is contained in datasets with the `report_` prefix, which are described in the table below.

The business side has their own method of naming dataset objects. This method is not meant to be implemented in the DWH side. 
However, all previous versions of datasets should still be available.

> **Note:** The business may request to rename business objects at any time in the future. In that case, the DWH Development team should create a new view in the database with the given new requirements.

## Delivery
1. Connect to AMOS Database via PostgreSQL Hook.
2. Call AMOS stored procedures* with the `dwh_heli_` prefix to get data and build datasets.
3. Deliver the given datasets to the DWH staging area.
4. Call stored procedures `sf_report_` to process data and publish the results to the presentation layer, depending on the object.

>*AMOS stored procedures have been implemented by the AMOS Development Team.

## List Of Datasets

| Dataset                    | Presentation Table                | View          | AMOS SP*     | DWH SP*                                      | ETL DAG                               | Status     |
| -------------------------- | --------------------------------- | ------------- | ------------ | -------------------------------------------- | ------------------------------------- | ---------- |
| general_staff_information  | report_general_staff_information  | UAE_SB_J_AA   | dwh_heli_006 | sf_prepare_report_general_staff_information  | dwh_report_general_staff_information  | Actual     |
| heli_status_repair         | report_heli_status_repair         | UAE_BA_B_RA   | dwh_heli_004 | sf_prepare_report_heli_status_repair         | dwh_report_status_repairs             | Actual     |
| helicopters_fleet          | report_helicopters_fleet          | UAE_AA_L_RA   | dwh_heli_008 | sf_prepare_report_helicopters_fleet          | dwh_report_helicopters_fleet          | Actual     |
| helicopters_station        | report_helicopters_station        | UAE_LA_Е_RA   | dwh_heli_007 | sf_prepare_report_helicopters_station        | dwh_report_helicopters_station        | Actual     |
| helicopters_utilization    | report_helicopters_utilization    | UAE_AA_K_RA   | dwh_heli_009 | sf_perform_report_helicopters_utilization    | dwh_report_helicopters_utilization    | Actual     |
| salary_piecework           | report_salary_piecework           | UAE_AA_A_RA   | dwh_heli_003 | sf_perform_report_salary_piecework           | dwh_report_salary_piecework           | Actual     |
| staff_qualification_status | report_staff_qualification_status | UAE_SA_I_AA   | dwh_heli_005 | sf_prepare_report_staff_qualification_status | dwh_report_staff_qualification_status | Actual     |

>*SP - Stored Procedure
