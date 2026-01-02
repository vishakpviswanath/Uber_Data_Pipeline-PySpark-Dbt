{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id',
        incremental_strategy = 'merge'
    )
}}


with base as (

    select
    {{ type_convert('trip_id', 'int') }} as trip_id,
    {{ type_convert('driver_id', 'int') }} as driver_id,
    {{ type_convert('customer_id', 'int') }} as customer_id,
    {{ type_convert('vehicle_id', 'int') }} as vehicle_id,
    {{ type_convert('trip_start_time', 'timestamp') }} as trip_start_time,
    {{ type_convert('trip_end_time', 'timestamp') }} as trip_end_time,
    {{ type_convert('last_updated_timestamp', 'timestamp') }} as last_updated_timestamp,
    {{ type_convert('distance_km', 'float') }} as distance_km,
    {{ type_convert('fare_amount', 'decimal') }} as fare_amount
from {{ source('source_bronze', 'bronze_trips') }}

),

deduped as (

    {{ deduplicate(
        'base',
        ['trip_id'],
        'last_updated_timestamp'
    ) }}

)


select
 trip_id,
 driver_id,
 customer_id,
 vehicle_id,
 trip_start_time,
 trip_end_time,
 last_updated_timestamp,
 distance_km,
 fare_amount,
{{ process_timestamp() }} as process_timestamp

from deduped

{% if is_incremental() %}
WHERE last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp),'1900-01-01') FROM {{ this }})
{% endif %}
