# Flight Ops Data Warehouse
> **Author:** [Konstantin Riazanov]()
> 
> **Email:** [konstantin.riazanov@engineer.com](mailto:konstantin.riazanov@engineer.com)
> 
> **LinkedIn:** [https://www.linkedin.com/in/konstantin-riazanov/](https://www.linkedin.com/in/konstantin-riazanov/)

## Description
Every company launches many business processes which involve a huge number of departments, various kinds of specialists, agents, partners, and other actors.
Air companies have their own departments and branches with various kinds of specialists, experts, and other employees who have different business needs and targets, from tech's and aircraft maintenance to economy and strategy departments.

This project is an example that demonstrates how to build a working data warehouse solution during the early R&D development process.
This is the first approach to building a data warehouse on RDBMS architecture, presented as is, with all links and sensitive information removed.
To implement this real existing case for a real existing air company, Ralph Kimball's* dimensional modeling techniques were used.

The key targets:
* To establish a working mechanism for the automated delivery, processing, and storage of data for analytical needs;
* To have the ability to test various BI tools to make informed decisions about purchasing;
* To check the ability to access data from legacy sources for our business users;
* To discover the solution's extensibility for new business requirements.

> &ast; "[The Data Warehouse Toolkit, 3rd Edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/)" by Ralph Kimball & Marhy Ross, Wiley, 2013.

## Project structure
* `storage` - contains simple dataset tables for new and legacy datasets, single reports, etc;
  * `dataset_group` - datasets can be arranged into groups, by department in our case;
    * `<dataset_name>` - all necessary coded database and ETL objects for proper execution;
      * `dag` - contains all necessary Airflow ETL DAGs;
      * `ddl` - contains DDL scripts for declaring database objects;
      * `dml` - contains DML code for data processing on the DB side using stored procedures;
* `warehouse` - contains data structure developed by dimensional modeling;
  * `datamarts` - logically grouped fact tables by business process;
    * `<datamart_name>` - contains all necessary code for creating `fact_` tables inside the data marts
      * `dag`
      * `ddl`
      * `dml`
  * `dimensions` - a list of dimension tables;
    * `<dimension_name>` - contains all necessary DDLs and DMLs for creating and populating `dim_` tables.

> **Note**: `storage` is an internal project term. All code in the `storage` directory has been developed without dimensional modeling techniques, but might be interesting as ETL processes.

## Warehouse Layers
* `dwh_staging` - contains tables and functions for data processing;
* `dwh_presentation` - contains tables and views that provide data access for business users.

## Stack information
* **Database**: PostgreSQL
* **ETL**: Apache Airflow

## Bus Matrix

|                                                                                                                 | [date](warehouse/dimensions/dim_date/dim_date.sql)  | [time](warehouse/dimensions/dim_time/dim_time.sql) | [flight](warehouse/dimensions/dim_flight/dim_flight.sql) | [board](warehouse/dimensions/dim_board/dim_board.sql) | [station](warehouse/dimensions/dim_station/dim_station.sql) | [crew_task_division](warehouse/dimensions/dim_crew_task_division/dim_crew_task_division.sql) | [crew_task_member](warehouse/dimensions/dim_crew_task_member/dim_crew_task_member.sql) | [flight_parameter](warehouse/dimensions/dim_flight_parameter/dim_flight_parameter.sql) | [runway](warehouse/dimensions/dim_runway/dim_runway.sql) | [junk](warehouse/dimensions/dim_junk/dim_junk.sql) |
| --------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------: | :------------------------------------------------: | :------------------------------------------------------: | :---------------------------------------------------: | :---------------------------------------------------------: | :------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------: | :------------------------------------------------------: | :------------------------------------------------: |
| **[leg](warehouse/datamarts/flight/ddl/fact_leg.sql)**                                                          | x                                                   | x                                                  | x                                                        | x                                                     | x                                                           |                                                                                              |                                                                                        |                                                                                        |                                                          |                                                    |
| **[segment](warehouse/datamarts/flight/ddl/fact_segment.sql)**                                                  | x                                                   | x                                                  | x                                                        | x                                                     | x                                                           |                                                                                              |                                                                                        |                                                                                        |                                                          |                                                    |
| **[crewtask](warehouse/datamarts/flight/ddl/fact_crewtask.sql)**                                                | x                                                   | x                                                  | x                                                        | x                                                     | x                                                           | x                                                                                            | x                                                                                      |                                                                                        |                                                          |                                                    |
| **[flight_millisecond](warehouse/datamarts/flight_millisecond/ddl/fact_flight_millisecond.sql)**                | x                                                   | x                                                  | x                                                        | x                                                     | x                                                           |                                                                                              |                                                                                        | x                                                                                      |                                                          |                                                    |
| **[takeoff_landing](warehouse/datamarts/takeoff_landing/ddl/fact_takeoff_landing.sql)**                             | x                                                   | x                                                  | x                                                        | x                                                     | x                                                           |                                                                                              | x                                                                                      |                                                                                        | x                                                        | x                                                  |

> **Note:** The whole objects are represented without prefixes `fact_` and `dim_`
