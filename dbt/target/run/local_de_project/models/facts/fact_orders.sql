
  
    
    

    create  table
      "warehouse"."main"."fact_orders__dbt_tmp"
  
    as (
      select
    o.order_id,
    c.customer_key,
    o.order_amount,
    o.order_status,
    cast(o.updated_at as date) as order_date
from stg_orders o
join dim_customer c
  on o.customer_id = c.customer_id
 and c.is_current = true
    );
  
  