-- The payoff table: what the analyst actually queries.
-- Every QC test upstream exists so THIS number can be trusted.
select
    o.order_date,
    count(distinct o.order_id) as orders,
    count(distinct o.collector_id) as collectors,
    sum(p.paid_amount) as revenue,
    sum(case when o.status = 'returned' then p.paid_amount else 0 end) as returned_revenue
from {{ ref('stg_orders') }} o
join {{ ref('stg_payments') }} p on o.order_id = p.order_id
group by 1
