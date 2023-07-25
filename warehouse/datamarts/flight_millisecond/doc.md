# Data Mart: "Flight Millisecond"

> **Customer:** Flight Safety Management Department
>  
> **Source System:** WinArm, Staffonly
> 
> **Fact type:** Transactional
> 
> **ETL DAG's:** `dwh_meridian_sap_xml_ops`
> 
> **DW/BI RAW table:** `raw_staffonly_files_decrypted`
> 
> **DW/BI Staging table:** `fact_flight_millisecond`
> 
> **DW/BI Presentation tables:** `fact_flight_millisecond`

## Description
Contains information from flight record. Fact table contains one millisecond of the flight.

## Delivery
1. Connect to Staffonly and withdraw all files from previous day;
2. All files found delivers to temporary created FTP on Windows OS and decryption WinArm utility.
3. Toggle winarm utility to decrypt recorder data, collect and upload to `raw_` table in staging area of data warehouse.

## Additional Info
### SQL debugging helper
```sql
select df.flight_no, db.tail_no, db.ac_type_name, dso.station_iata as origin, dsd.station_iata as destination,
       flight_date, actual_date, delay_codes, departure_plan_local, departure_plan_utc, arrival_plan_local,
       arrival_plan_utc, departure_est_local, departure_est_utc, arrival_est_local, arrival_est_utc,
       departure_fact_local, departure_fact_utc, arrival_fact_local, arrival_fact_utc, power_on, power_off, on_surface,
       pax_transit, f_nr, c_nr, y_nr, child_nr, infant_nr, crg_load, luggage_load, hand_luggage, mail_load,
       cabin_baggage, emptying, fueling, dep_remnant, arr_remnant, f_seats_occupied, c_seats_occupied, y_seats_occupied,
       f_seats_total, c_seats_total, y_seats_total, payload, f_child, c_child, y_child, f_infant, c_infant, y_infant,
       distance, equipped_empty_kg, flight_task, external_flight_id, fuel_qty_on_flight_record_start, fuel_qty_on_flight_record_finish,
       aircraft_weight_pounds_start_record, aircraft_weight_pounds_finish_record, aircraft_weight_tons_start_record,
       aircraft_weight_tons_finish_record, aircraft_weight_without_fuel_tons_start_record, aircraft_weight_without_fuel_tons_finish_record
from dwh_presentation.fact_leg fl
left join dwh_presentation.dim_flight df on df.id = fl.id_flight
left join dwh_presentation.dim_board db on db.id = fl.id_board
left join dwh_presentation.dim_station dso on dso.id = fl.id_origin
left join dwh_presentation.dim_station dsd on dsd.id = fl.id_destination
where 1=1
and id_date between 20220501 and 20220531;
```
