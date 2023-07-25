# Data Mart: "Flight"

> **Customer:** Analytic Department
>  
> **Source System:** Meridian
> 
> **Fact type:** Transactional
> 
> **ETL DAG's:** `dwh_meridian_sap_xml_ops`, `dwh_meridian_sap_xml_sal`
> 
> **DW/BI RAW tables:** `raw_meridian_board_xml`, `raw_meridian_sap_xml`
> 
> **DW/BI Staging tables:** `fact_leg_ops`, `fact_leg_sal`, `fact_segment`, `fact_crewtask`
> 
> **DW/BI Presentation tables:** `fact_leg`, `fact_segment`, `fact_crewtask`

## Datamart Description
Datamart contains commercial information about every single flight: passenger counts, cargo and mail, fuelling etc.

### Fact Table Description: `fact_leg`
Every flight can be directed(A->B->A) or chained(A->B->C), so one flight can be granulated to physically take-off and landing event into one line of fact table.

### Fact Table Description: `fact_segment`
Passengers can buy a ticket from each station to each station of the route.
Those passengers named as **transfer** passengers (same with cargo/mail).

Segment should be explained by example.
In case of abstract flight route A->B->C->D, we have 3 legs:
1. A->B;
2. B->C;
3. C->D.

But **segments** count is 6: 
1. A->B
2. A->C
3. A->D
4. B->C
5. B->D
6. C->D

### Fact Table Description: `fact_crewtask`
Keeps information about flight and cabin crews.
Granularity is **flight plus crew member** of both cabin and flight crew.

## Delivery
1. All data drives from two sources which is generating from Meridian source system side:
   * `board.xml` - data for online departure board, contains planned flights, refreshes every 5 minutes, uploads to DWH every 6 hours;
   * `exp_YYYYMMDD_sap.xml` - contains data with closed flight task (all flight information filled and checked already by Flight Information Processing Department);
2. Each file should be uploaded to `raw_` tables inside staging area;
3. Extract data from `raw_` tables to `fact_leg_xxx` tables;
4. Update already existing flights (merging data);
5. Publish data to presentation layer.
