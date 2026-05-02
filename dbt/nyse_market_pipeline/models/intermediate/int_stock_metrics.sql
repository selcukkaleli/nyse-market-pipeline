with source as (
    select * from {{ ref('stg_stocks_enriched') }}
),

final as (
    select
        symbol,
        stock_year,
        sector,
        sub_industry,
        company_name,
        period_ending,
        net_income,
        gross_margin,
        operating_margin,
        eps,
        ((last_close-first_close)/first_close) AS yearly_return,
        (avg_daily_range/first_close) AS volatility,
        ((last_close-first_close)/first_close) / (avg_daily_range/first_close) AS risk_adj_return,
        (long_term_debt / total_equity) AS debt_to_equity

    from source
    
)

select * from final
