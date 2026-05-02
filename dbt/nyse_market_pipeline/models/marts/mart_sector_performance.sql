with source as (
    select * from {{ ref('int_stock_metrics') }}
),

final as (
    select
        sector,
        AVG(yearly_return) AS avg_yearly_return,
        AVG(volatility) AS avg_volatility,
        AVG(risk_adj_return) AS avg_risk_adj_return

    from source
    GROUP BY sector
)

select * from final