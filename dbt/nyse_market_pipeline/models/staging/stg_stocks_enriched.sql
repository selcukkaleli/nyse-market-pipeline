with source as (
    select * from {{ source('nyse', 'stocks_enriched') }}
),

renamed as (
    select 

        symbol,
        "year" as stock_year,
        first_close,
        last_close,
        avg_daily_range,
        sector,
        sub_industry,
        company_name,
        period_ending,
        total_revenue,
        net_income,
        gross_margin,
        operating_margin,
        eps,
        long_term_debt,
        total_equity

         from source
)

select * from renamed;