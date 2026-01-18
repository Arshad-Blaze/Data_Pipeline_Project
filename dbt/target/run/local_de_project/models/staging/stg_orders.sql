
  
  create view "warehouse"."main"."stg_orders__dbt_tmp" as (
    select * from stg_orders
  );
