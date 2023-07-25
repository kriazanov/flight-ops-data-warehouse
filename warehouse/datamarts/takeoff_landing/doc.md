# Data Mart: "Takeoff/Landing"

> **Customer:** Flight Safety Management Department
>  
> **Source System:** Skytrac
> 
> **Fact type:** Transactional
> 
> **ETL DAG's:** `dwh_skytrac_emails`
> 
> **DW/BI RAW table:** `raw_skytrac_mails`
> 
> **DW/BI Staging table:** `fact_takeoff_landing`
> 
> **DW/BI Presentation tables:** `fact_takeoff_landing`

## Description
Every flight can be decomposed further into takeoff and landing events. 
Each line of the fact table represents either a take-off or landing event for a flight. 
For proper processing, it requires runway information with runway numbers, tracks, geo coordinates, and more. 
Therefore, a dimension table `dim_runway` was manually prepared and uploaded to the data warehouse.

## Delivery
1. Airplanes send information about takeoff or landing to an email address.
2. The Airflow DAG `dwh_skytrac_mails` reads the mail address every 5 minutes and retrieves all unseen mails.
3. Each email is parsed and uploaded to the `raw_` table in the staging area of the data warehouse.
4. The data should be enriched from the `flight` data mart:
   - `flight_number` from `fact_leg`
   - `captain_name` from `fact_crewtask`
   - `airport_name` from `dim_station`
5. The enriched data should be published to the presentation layer.
