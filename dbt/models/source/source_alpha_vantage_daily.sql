
with source as (
    select * from {{ source('alpha_vantage', 'alpha_vantage_daily') }}
),

casted as (
select
cast(date as date) as date,
 ticker,
 cast(open as numeric(18, 4)) as open_price,
 cast(high as numeric(18, 4)) as high_price,
 cast(low as numeric(18, 4)) as low_price,
 cast(close as numeric(18, 4)) as close_price,
 cast(volume as integer) as volume
from source
)

select * from casted
