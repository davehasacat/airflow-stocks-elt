with source as (

    select * from {{ source('polygon', 'source_stocks_polygon_daily_bars') }}

),

renamed_and_casted as (
select
 cast(date as date) as date,
 ticker,
 cast(open_price as numeric(18, 4)) as open_price,
 cast(high_price as numeric(18, 4)) as high_price,
 cast(low_price as numeric(18, 4)) as low_price,
 cast(close_price as numeric(18, 4)) as close_price,
 cast(volume as integer) as volume
from source
)

select * from renamed_and_casted
