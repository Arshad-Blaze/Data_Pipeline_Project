select
    row_number() over () as customer_key,
    customer_id,
    current_timestamp as effective_from,
    timestamp '9999-12-31' as effective_to,
    true as is_current
from (
    select distinct customer_id
    from stg_orders
)