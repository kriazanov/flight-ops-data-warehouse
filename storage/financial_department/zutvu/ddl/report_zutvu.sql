drop table dwh_staging.report_zutvu;
create table dwh_staging.report_zutvu
(
    wknam__work_name_text varchar,
    number__number_flight_task varchar,
    date__flight_task_date date,
    carrier__carrier_code varchar,
    carrier_txt__carrier_name varchar,
    bstkd_e__flight_number varchar,
    litera__flight_litera varchar,
    fbuda__work_date date,
    bstkd__request_number varchar,
    bstdk__request_date date,
    customer__customer_code varchar,
    customer_name__customer_name varchar,
    customer_unit__customer_subdivision_code varchar,
    customer_unit_name__customer_subdivision_name varchar,
    contract__contract varchar,
    external_contract__contract_date_from_card varchar,
    vkbur__direction int,
    aircraft__aircraft varchar,
    aircraft_type__aircraft_type varchar,
    aircraft_owner__aircraft_owner_code varchar,
    aircraft_owner_txt__aircraft_owner_txt varchar,
    airct_c__board_act varchar,
    airct_type_c__board_type_act varchar,
    zzcitya_alv__city_code_work varchar,
    zzcitya_alv_txt__city_text_work varchar,
    eartim__ground_time varchar,
    earmng__ground_count float,
    airtim__air_time varchar,
    airmng__air_count float,
    sumtim__sum_time varchar,
    summng__sum_count float,
    typnp_txt__non_prod_flight_type varchar,
    nptim__non_prod_flight_time varchar,
    npmng__non_prod_flight_count float,
    actdur__act_time varchar,
    actmng__act_count float,
    sum_actdur__act_sum_time varchar,
    sum_actmng__act_sum_count float,
    order__order_number varchar,
    matxt__full_material_name varchar,
    summa__sum_without_nds float,
    nds_sum__with_nds float,
    sum_diff__accuracy float,
    waerk__currency varchar,
    order_act_c__task_number_act varchar,
    vbeln_vf__facture_number varchar,
    vbeln_vf_c__facture_number_act varchar,
    zieme__unit varchar,
    fltid__flight_id int,
    zzextnum__external_invoice_number varchar,
    bldat__document_date varchar,
    comments__comments varchar,
    delta_aircttype__delta_flight varchar,
    delta_acttime__delta_fact_act varchar,
    uname__creator_name varchar
);

alter table dwh_staging.report_zutvu owner to dwh;

DROP TABLE dwh_presentation.report_zutvu;
CREATE TABLE dwh_presentation.report_zutvu
(
    flight_id int,
    work_name_text varchar,
    number_flight_task varchar,
    flight_task_date date,
    carrier_code varchar,
    carrier_name varchar,
    flight_number varchar,
    work_date date,
    request_number varchar,
    request_date date,
    customer_code varchar,
    customer_name varchar,
    customer_subdivision_code varchar,
    customer_subdivision_name varchar,
    contract varchar,
    contract_date_from_card varchar,
    direction int,
    aircraft varchar,
    aircraft_type varchar,
    aircraft_owner_code varchar,
    aircraft_owner_txt varchar,
    board_act varchar,
    board_type_act varchar,
    ground_time varchar,
    ground_count float,
    air_time varchar,
    air_count float,
    sum_time varchar,
    sum_count float,
    non_prod_flight_type varchar,
    non_prod_flight_time varchar,
    non_prod_flight_count float,
    act_time varchar,
    act_count float,
    act_sum_time varchar,
    act_sum_count float,
    order_number varchar,
    full_material_name varchar,
    sum_without_nds float,
    sum_with_nds float,
    accuracy float,
    task_number_act varchar,
    facture_number varchar,
    facture_number_act varchar,
    delta_flight varchar,
    delta_fact_act varchar,
    flight_litera varchar,
    city_code_work varchar,
    city_text_work varchar,
    currency varchar,
    unit varchar,
    external_invoice_number varchar,
    document_date varchar,
    comments_text varchar,
    creator_name varchar
);

alter table dwh_presentation.report_zutvu owner to dwh;
grant select on dwh_presentation.report_zutvu to readonly;