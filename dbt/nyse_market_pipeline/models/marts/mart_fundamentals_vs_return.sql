with source as (
    select * from {{ ref('int_stock_metrics') }}
),

final as (
    select
        company_name,
        stock_year,
        debt_to_equity,
        eps,
        gross_margin,
        yearly_return

    from source

)

select * from final