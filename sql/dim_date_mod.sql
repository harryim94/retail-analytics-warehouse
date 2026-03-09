alter table mart.dim_date
  add column if not exists day int,
  add column if not exists year_month text,
  add column if not exists quarter int,
  add column if not exists quarter_label text,
  add column if not exists iso_year int,
  add column if not exists iso_week int,
  add column if not exists iso_dow int,
  add column if not exists dow_name text,
  add column if not exists is_month_start boolean,
  add column if not exists is_month_end boolean;
/*
year, month, weekday, is_weekend는 이미 있으니까 안 덮어씀.
대신 “검증/정합성” 관점에서 day, year_month, quarter, iso_*, month_start/end를 채워.
*/
  update mart.dim_date d
set
  day = extract(day from d.sale_date)::int,
  year_month = to_char(d.sale_date, 'YYYY-MM'),

  quarter = extract(quarter from d.sale_date)::int,
  quarter_label = ('Q' || extract(quarter from d.sale_date)::int)::text,

  iso_year = extract(isoyear from d.sale_date)::int,
  iso_week = extract(week from d.sale_date)::int,

  -- iso_dow: 1=Mon ... 7=Sun (표준에 가까움)
  iso_dow = extract(isodow from d.sale_date)::int,
  dow_name = to_char(d.sale_date, 'Dy'),

  is_month_start = (d.sale_date = date_trunc('month', d.sale_date)::date),
  is_month_end   = (d.sale_date = (date_trunc('month', d.sale_date) + interval '1 month - 1 day')::date);

  --validation
select sale_date, weekday,
       (extract(isodow from sale_date)::int - 1) as computed_weekday
from mart.dim_date
where weekday <> (extract(isodow from sale_date)::int - 1);
  
  select g.d::date as missing_date
from generate_series(
  (select min(sale_date) from mart.dim_date),
  (select max(sale_date) from mart.dim_date),
  interval '1 day'
) g(d)
left join mart.dim_date d
  on d.sale_date = g.d::date
where d.sale_date is null
order by 1;

/*
filling up missing date 
*/
insert into mart.dim_date (sale_date)
select g.d::date
from generate_series(
  (select min(sale_date) from mart.dim_date),
  (select max(sale_date) from mart.dim_date),
  interval '1 day'
) g(d)
left join mart.dim_date d
  on d.sale_date = g.d::date
where d.sale_date is null;

update mart.dim_date d
set
  year = extract(year from d.sale_date)::int,
  month = extract(month from d.sale_date)::int,
  weekday = (extract(isodow from d.sale_date)::int - 1),
  is_weekend = (extract(isodow from d.sale_date) in (6,7));

  update mart.dim_date d
set
  day = extract(day from d.sale_date)::int,
  year_month = to_char(d.sale_date, 'YYYY-MM'),
  quarter = extract(quarter from d.sale_date)::int,
  iso_year = extract(isoyear from d.sale_date)::int,
  iso_week = extract(week from d.sale_date)::int,
  iso_dow = extract(isodow from d.sale_date)::int,
  dow_name = to_char(d.sale_date, 'Dy'),
  is_month_start = (d.sale_date = date_trunc('month', d.sale_date)::date),
  is_month_end   = (d.sale_date = (date_trunc('month', d.sale_date) + interval '1 month - 1 day')::date);


  select count(*) from mart.dim_date;

  update mart.dim_date d
set
  year = extract(year from d.sale_date)::int,
  month = extract(month from d.sale_date)::int,
  weekday = (extract(isodow from d.sale_date)::int - 1), -- Mon=0..Sun=6
  is_weekend = (extract(isodow from d.sale_date) in (6,7));

  select
  d.sale_date,
  coalesce(sum(f.net_quantity), 0) as total_net_quantity,
  coalesce(sum(f.net_revenue_local), 0) as total_net_revenue
from mart.dim_date d
left join mart.sales_fact f
  on f.sale_date = d.sale_date
group by d.sale_date
order by d.sale_date;