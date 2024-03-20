{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_2019') }}
  where dispatching_base_num is not null 
)

--total:gcs:43244696
--total:gcs:42084899
select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    {{ dbt.safe_cast("PUlocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    SR_Flag,
    Affiliated_base_number
from tripdata
where pickup_datetime >= ('2019-01-01') and pickup_datetime < ('2020-01-01')
and dispatching_base_num is not null 