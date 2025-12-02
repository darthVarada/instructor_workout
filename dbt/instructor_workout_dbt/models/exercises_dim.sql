select *
from {{ source('gold', 'exercises_dim') }}