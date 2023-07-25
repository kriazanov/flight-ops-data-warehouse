create or replace function dwh_staging.sf_transform_sap_zut_bill() returns void
    language plpgsql
as
$$
begin
    truncate table dwh_staging.report_zut_bill;
    truncate table dwh_staging.dict_zut_bill;
    truncate table dwh_presentation.report_zut_bill;

    insert into dwh_staging.report_zut_bill(
        sales_channel_code, sales_channel_name, trade_doc, external_invoice_num, sales_doc_pos, sales_doc,
        sales_doc_number, sales_organization_name, fp_order_code, fp_order_name, fp_accounting_doc_code,
        fp_accounting_doc_name, material_number, material_group_name, sales_dept_name, airport_oirg_name,
        aircraft_act_type, aircraft_type, aircraft, owner_code, order_number, order_number_external,
        actual_invoiced_quantity, selling_mu, net_price, net_currency_doc_curr, customer_code, customer_name,
        payer_name, doc_executor_name, client_sales_text, works_services_completion_date, flight_task_date,
        record_created_date, sales_doc_curr, exchange_rate_for_fi_postings, reversed_invoice_number, flight_task_number,
        invoice_kind_code, material_recipient_name, city_name, sales_organization_code, material_group_code,
        sales_dept_code, payer_code, material_recipient_code, company_group, company_group_name)
    select
        nullif(vtweg, '') as sales_channel_code,
        nullif(vtweg_text, '') as sales_channel_name,
        nullif(aubel, '') as trade_doc,
        nullif(zzextnum, '') as external_invoice_num,
        nullif(posnr, '') as sales_doc_pos,
        nullif(vbeln, '') as sales_doc,
        nullif(belnr, '') as sales_doc_number,
        nullif(vkorg_text, '') as sales_organization_name,
        nullif(fpord, '') as fp_order_code,
        nullif(fpord_text, '') as fp_order_name,
        nullif(fpfid, '') as fp_accounting_doc_code,
        nullif(fpfid_text, '') as fp_accounting_doc_name,
        nullif(matnr, '') as material_number,
        nullif(matnr_text, '') as material_group_name,
        nullif(vkbur_text, '') as sales_dept_name,
        nullif(zzwwade, '') as airport_oirg_name,
        nullif(zzwwtvs_a, '') as aircraft_act_type,
        nullif(zzwwtvs, '') as aircraft_type,
        nullif(zzequnr, '') as aircraft,
        nullif(owner, '') as owner,
        nullif(zcntrregnumdo, '') as order_number,
        nullif(zcntrnum_prtn, '') as order_number_external,
        nullif(fkimg, '') as actual_invoiced_quantity,
        nullif(vrkme, '') as selling_mu,
        nullif(netpr, '') as net_price,
        nullif(netwr_vbrp, '') as net_currency_doc_curr,
        nullif(kunag, '') as customer_code,
        nullif(kunag_text, '') as customer_name,
        nullif(kunrg_text, '') as payer_name,
        nullif(ernam, '') as doc_executor_name,
        nullif(matnr, '') as client_sales_text,
        nullif(fbuda, '') as works_services_completion_date,
        nullif(zzpzdt, '') as flight_task_date,
        nullif(erdat, '') as record_created_date,
        nullif(waerk, '') as sales_doc_curr,
        nullif(kurrf, '') as exchange_rate_for_fi_postings,
        nullif(sfakn, '') as reversed_invoice_number,
        nullif(zzpznr, '') as flight_task_number,
        nullif(fkart, '') as invoice_kind_code,
        nullif(kunwe_text, '') as material_recipient_name,
        nullif(zzcitya, '') as city_name,
        nullif(sales_organization_code, '') as sales_organization_code,
        nullif(material_group_code, '') as material_group_code,
        nullif(sales_dept_code, '') as sales_dept_code,
        nullif(kunrg, '') as payer_code,
        nullif(kunwe, '') as material_recipient_code,
        nullif(bp_group, '') as company_group,
        nullif(name_group, '') as company_group_name

    from dwh_staging.raw_sap_zut_bill_soap_response,
    xmltable('//ES_DATA/MAIN/item' passing response_content columns
        vtweg varchar path 'VTWEG',
        belnr varchar path 'BELNR',
        matnr_text varchar path 'MATNR_TEXT',
        bp_group varchar path 'BP_GROUP',
        name_group varchar path 'NAME_GROUP',
        fpord_text varchar path 'FPORD_TEXT',
        aufnr varchar path 'AUFNR',
        zzwwade varchar path 'ZZWWADE',
        vbeln varchar path 'VBELN',
        posnr varchar path 'POSNR',
        fkart varchar path 'FKART',
        waerk varchar path 'WAERK',
        sales_organization_code varchar path 'VKORG',
        vkorg_text varchar path 'VKORG_TEXT',
        zzpznr varchar path 'ZZPZNR',
        vtweg_text varchar path 'VTWEG_TEXT',
        kurrf varchar path 'KURRF',
        ernam varchar path 'ERNAM',
        erdat varchar path 'ERDAT',
        kunrg varchar path 'KUNRG',
        kunrg_text varchar path 'KUNRG_TEXT',
        kunag varchar path 'KUNAG',
        kunag_text varchar path 'KUNAG_text',
        kunwe varchar path 'KUNWE',
        kunwe_text varchar path 'KUNWE_TEXT',
        sfakn varchar path 'SFAKN',
        fkimg varchar path 'FKIMG',
        vrkme varchar path 'VRKME',
        fbuda varchar path 'FBUDA',
        netpr varchar path 'NETPR',
        netwr_vbrp varchar path 'NETWR_VBRP',
        aubel varchar path 'AUBEL',
        matnr varchar path 'MATNR',
        material_group_code varchar path 'MATKL',
        fpord varchar path 'FPORD',
        fpfid varchar path 'FPFID',
        fpfid_text varchar path 'FPFID',
        sales_dept_code varchar path 'VKBUR',
        vkbur_text varchar path 'VKBUR_TEXT',
        zzpzdt varchar path 'ZZPZDT',
        zzwwtvs_a varchar path 'ZZWWTVS_A',
        zzextnum varchar path 'ZZEXTNUM',
        zzequnr varchar path 'ZZEQUNR',
        zzwwtvs varchar path 'ZZWWTVS',
        zcntrregnumdo varchar path 'ZCNTRREGNUMDO',
        zcntrnum_prtn varchar path 'ZCNTRNUM_PRTN',
        owner varchar path 'OWNER',
        zzcitya varchar path 'ZZCITYA');

    insert into dwh_staging.dict_zut_bill
    select
        nullif(tag, '') as tag,
        nullif(t_key, '') as t_key,
        nullif(t_val, '') as t_val
    from dwh_staging.raw_sap_zut_bill_soap_response,
        xmltable('//ES_DATA/TEXT/item' passing response_content columns
            tag varchar path 'TAG',
            t_key varchar path 'T_KEY',
            t_val varchar path 'T_VAL');

    insert into dwh_presentation.report_zut_bill
    select
        report.sales_channel_code as sales_channel_code,
        vtweg.t_val as sales_channel_name,
        report.trade_doc::int as trade_doc,
        report.external_invoice_num::int as external_invoice_num,
        report.sales_doc_pos::int as sales_doc_pos,
        report.sales_doc::int as sales_doc,
        vkorg.t_val as sales_organization_name,
        report.fp_order_code as fp_order_code,
        fpord.t_val as fp_order_name,
        report.fp_accounting_doc_code as fp_accounting_doc_code,
        fpfid.t_val as fp_accounting_doc_name,
        report.material_number::int as material_number,
        matkl.t_val as material_group_name,
        vkbur.t_val as sales_dept_name,
        zzwwade.t_val as airport_oirg_name,
        report.aircraft_act_type as aircraft_act_type,
        report.aircraft_type as aircraft_type,
        report.aircraft as aircraft,
        report.owner_code::int as owner_code,
        own.t_val as owner_name,
        report.order_number as order_number,
        report.order_number_external as order_number_external,
        report.actual_invoiced_quantity::float as actual_invoiced_quantity,
        dsmu.mseh3 as selling_mu,
        report.net_price::float as net_price,
        report.net_currency_doc_curr::float as net_currency_doc_curr,
        report.customer_code as customer_code,
        kunag.t_val as customer_name,
        kunrg.t_val as payer_name,
        report.doc_executor_name as doc_executor_name,
        matnr.t_val as client_sales_text,
        to_date(report.works_services_completion_date, 'YYYY-MM-DD') as works_services_completion_date,
        to_date(report.flight_task_date, 'YYYY-MM-DD') as flight_task_date,
        to_date(report.record_created_date, 'YYYY-MM-DD') as record_created_date,
        report.sales_doc_curr as sales_doc_curr,
        report.exchange_rate_for_fi_postings::float as exchange_rate_for_fi_postings,
        report.reversed_invoice_number as reversed_invoice_number,
        report.flight_task_number as flight_task_number,
        report.invoice_kind_code as invoice_kind_code,
        kunwe.t_val as material_recipient_name,
        zzcitya.t_val as city_name,
        report.company_group as company_group,
        report.company_group_name as company_group_name,
        report.sales_doc_number as sales_doc_number

    from dwh_staging.report_zut_bill report
    left join dwh_staging.dict_zut_bill own on own.tag = 'OWNER' and own.t_key = report.owner_code
    left join dwh_staging.dict_zut_bill fkart on fkart.tag = 'FKART' and fkart.t_key = report.invoice_kind_code
    left join dwh_staging.dict_zut_bill vkorg on vkorg.tag = 'VKORG' and vkorg.t_key = report.sales_organization_code
    left join dwh_staging.dict_zut_bill vtweg on vtweg.tag = 'VTWEG' and vtweg.t_key = report.sales_channel_code
    left join dwh_staging.dict_zut_bill kunrg on kunrg.tag = 'KUNRG' and kunrg.t_key = report.payer_code
    left join dwh_staging.dict_zut_bill kunag on kunag.tag = 'KUNAG' and kunag.t_key = report.customer_code
    left join dwh_staging.dict_zut_bill kunwe on kunwe.tag = 'KUNWE' and kunwe.t_key = report.material_recipient_code
    left join dwh_staging.dict_zut_bill matkl on matkl.tag = 'MATKL' and matkl.t_key = report.material_group_code
    left join dwh_staging.dict_zut_bill fpord on fpord.tag = 'FPORD' and fpord.t_key = regexp_replace(report.fp_order_code, '\.', '', 'g')
    left join dwh_staging.dict_zut_bill fpfid on fpfid.tag = 'FPFID' and fpfid.t_key = regexp_replace(report.fp_accounting_doc_code, '\.', '', 'g')
    left join dwh_staging.dict_zut_bill vkbur on vkbur.tag = 'VKBUR' and vkbur.t_key = report.sales_dept_code
    left join dwh_staging.dict_zut_bill zzwwade on zzwwade.tag = 'ZZWWADE' and regexp_replace(report.airport_oirg_name, '\.', '', 'g') = zzwwade.t_key
    left join dwh_staging.dict_zut_bill zzcitya on zzcitya.tag = 'ZZCITYA' and zzcitya.t_key = report.city_name
    left join dwh_staging.dict_zut_bill matnr on matnr.tag = 'MATNR' and matnr.t_key = report.client_sales_text
    left join dwh_staging.dict_sap_measure_unit dsmu on dsmu.msehi = report.selling_mu;
end
$$;

alter function dwh_staging.sf_transform_sap_zut_bill() owner to dwh;

