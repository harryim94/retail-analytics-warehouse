select * from mart.sales_fact;
--KPI for total quanity and revenue per date
select sale_date,sum(net_quantity) as total_net_quantity, sum(net_revenue_local) as total_net_revenue
from mart.sales_fact
group by sale_date
order by sale_date;

--KPI for top 10 products
with sku_rev as (
  select
    sku,
    sum(net_revenue_local) as total_net_revenue
  from mart.sales_fact
  group by sku
),
ranked as (
  select
    sku,
    total_net_revenue,
    dense_rank() over (order by total_net_revenue desc) as rnk
  from sku_rev
)
select sku, total_net_revenue, rnk
from ranked
where rnk <= 10
order by rnk, total_net_revenue desc, sku;

--kpi for cancel rate
with sku_cancel as (
  select
    sku,
    sum(gross_quantity)          as gross_qty,
    sum(cancelled_quantity)      as cancelled_qty,
    sum(gross_revenue_local)     as gross_rev,
    sum(cancelled_revenue_local) as cancelled_rev
  from mart.sales_fact
  group by sku
)
select
  sku,
  gross_qty,
  cancelled_qty,
  cancelled_rev,
  gross_rev,
  cancelled_qty / nullif(gross_qty, 0)::numeric as cancel_rate_qty,
  cancelled_rev / nullif(gross_rev, 0)::numeric as cancel_rate_revenue
from sku_cancel
order by cancel_rate_revenue desc;


--kpi for monthly trends
select
  t2.year,
  t2.month,
  sum(t1.net_revenue_local) as total_net_revenue
from mart.sales_fact t1
join mart.dim_date t2
  on t1.sale_date = t2.sale_date
group by t2.year, t2.month
order by t2.year, t2.month;

--kpi for daily trends
with daily as (
  select
    d.sale_date,
    d.iso_dow,
    d.dow_name,
    coalesce(sum(f.net_revenue_local), 0) as daily_revenue
  from mart.dim_date d
  left join mart.sales_fact f
    on f.sale_date = d.sale_date
  group by d.sale_date, d.iso_dow, d.dow_name
)
select
  iso_dow,
  dow_name,
  avg(daily_revenue) as avg_daily_revenue,
  sum(daily_revenue) as total_revenue,
  count(*) as num_days
from daily
group by iso_dow, dow_name
order by iso_dow;


