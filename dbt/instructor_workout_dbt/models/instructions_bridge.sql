select *
from {{ source('gold', 'instructions_bridge') }}