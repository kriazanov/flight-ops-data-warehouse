create or replace function dwh_staging.sf_dim_date(p_id integer)
returns integer
language plpgsql
as $$
begin
    insert into dwh_presentation.dim_date(
        id, date, date_str,
        year, year_short, half_year_code, half_year_name, quarter_no, quarter_code, quarter_name,
        month_number, month_uppercase, month_capitalized, month_upper_short, month_capitalized_short,
        week_month, week_year,
        day_week_uppercase, day_week, day_short_uppercase, day_short, day_year, day_month, day_week_number)
    select distinct p_id,
        --date
        to_date(p_id::varchar, 'YYYYMMDD')::date as date,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'DD.MM.YYYY') as date_str,
        --year
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'YYYY') as year,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'YY') as year_short,
        --half_year
        case
            when extract(month from to_date(p_id::varchar, 'YYYYMMDD')::date) between 1 and 6 then 'HY1'
            when extract(month from to_date(p_id::varchar, 'YYYYMMDD')::date) between 7 and 12 then 'HY2'
        end as half_year_code,
        case
            when extract(month from to_date(p_id::varchar, 'YYYYMMDD')::date) between 1 and 6 then 'Первое'
            when extract(month from to_date(p_id::varchar, 'YYYYMMDD')::date) between 7 and 12 then 'Второе'
        end as half_year_name,
        --quarter
        extract(quarter from to_date(p_id::varchar, 'YYYYMMDD')::date) as quarter_no,
        case extract(quarter from to_date(p_id::varchar, 'YYYYMMDD')::date)
            when 1 then 'Q1'
            when 2 then 'Q2'
            when 3 then 'Q3'
            when 4 then 'Q4'
        end as quarter_code,
        case extract(quarter from to_date(p_id::varchar, 'YYYYMMDD')::date)
            when 1 then 'Первый'
            when 2 then 'Второй'
            when 3 then 'Третий'
            when 4 then 'Четвертый'
        end as quarter_name,
        --month
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'MM') as month_number,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'MONTH') as month_uppercase,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'Month') as month_capitalized,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'MON') as month_upper_short,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'Mon') as month_capitalized_short,
        --week
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'W') as week_month,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'WW') as week_year,
        --day
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'DAY') as day_week_uppercase,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'Day') as day_week,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'DY') as day_short_uppercase,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'Dy') as day_short,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'DDD') as day_year,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'DD') as day_month,
        to_char(to_date(p_id::varchar, 'YYYYMMDD')::date, 'ID') as day_week_number
    on conflict(id) do nothing;
    return p_id;
end;
$$;