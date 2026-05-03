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

    ORDER BY company_name ASC, stock_year ASC
)

select * from final