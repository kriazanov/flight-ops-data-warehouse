do
$dwh_staging$
    begin
        drop table dwh_staging.report_zut_bill;
        create table report_zut_bill
        (
            sales_channel_code             varchar,
            sales_channel_name             varchar,
            trade_doc                      varchar,
            external_invoice_num           varchar,
            sales_doc_pos                  varchar,
            sales_doc                      varchar,
            sales_organization_name        varchar,
            fp_order_code                  varchar,
            fp_order_name                  varchar,
            fp_accounting_doc_code         varchar,
            fp_accounting_doc_name         varchar,
            material_number                varchar,
            material_group_name            varchar,
            sales_dept_name                varchar,
            airport_oirg_name              text,
            aircraft_act_type              varchar,
            aircraft_type                  varchar,
            aircraft                       varchar,
            owner_code                     varchar,
            owner_name                     varchar,
            order_number                   varchar,
            order_number_external          varchar,
            actual_invoiced_quantity       varchar,
            selling_mu                     varchar,
            net_price                      varchar,
            net_currency_doc_curr          varchar,
            customer_code                  varchar,
            customer_name                  varchar,
            payer_name                     varchar,
            doc_executor_name              varchar,
            client_sales_text              varchar,
            works_services_completion_date varchar,
            flight_task_date               varchar,
            record_created_date            varchar,
            sales_doc_curr                 varchar,
            exchange_rate_for_fi_postings  varchar,
            reversed_invoice_number        varchar,
            flight_task_number             varchar,
            invoice_kind_code              varchar,
            material_recipient_name        varchar,
            city_name                      varchar,
            sales_organization_code        varchar,
            material_group_code            varchar,
            sales_dept_code                varchar,
            payer_code                     varchar,
            material_recipient_code        varchar,
            company_group                  varchar,
            company_group_name             varchar
        );

        alter table dwh_staging.report_zut_bill
            owner to dwh;
    end;
$dwh_staging$;

do
$dwh_presentation$
    begin
        drop table dwh_presentation.report_zut_bill;
        create table dwh_presentation.report_zut_bill
        (
            sales_channel_code             varchar,
            sales_channel_name             varchar,
            trade_doc                      integer,
            external_invoice_num           integer,
            sales_doc_pos                  integer,
            sales_doc                      integer,
            sales_organization_name        varchar,
            fp_order_code                  varchar,
            fp_order_name                  varchar,
            fp_accounting_doc_code         varchar,
            fp_accounting_doc_name         varchar,
            material_number                integer,
            material_group_name            varchar,
            sales_dept_name                varchar,
            airport_oirg_name              text,
            aircraft_act_type              varchar,
            aircraft_type                  varchar,
            aircraft                       varchar,
            owner_code                     integer,
            owner_name                     varchar,
            order_number                   varchar,
            order_number_external          varchar,
            actual_invoiced_quantity       float,
            selling_mu                     varchar,
            net_price                      float,
            net_currency_doc_curr          float,
            customer_code                  varchar,
            customer_name                  varchar,
            payer_name                     varchar,
            doc_executor_name              varchar,
            client_sales_text              varchar,
            works_services_completion_date date,
            flight_task_date               date,
            record_created_date            date,
            sales_doc_curr                 varchar,
            exchange_rate_for_fi_postings  float,
            reversed_invoice_number        varchar,
            flight_task_number             varchar,
            invoice_kind_code              varchar,
            material_recipient_name        varchar,
            city_name                      varchar,
            company_group                  varchar,
            company_group_name             varchar
        );

        alter table dwh_presentation.report_zut_bill
            owner to dwh;
    end;
$dwh_presentation$;
