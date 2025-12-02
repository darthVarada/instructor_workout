select
    e.exercise_id,
    e.exercise_name,
    e.equipment,
    e.main_muscle,
    m.muscle,
    i.step_number,
    i.instruction
from {{ source('gold', 'exercises_dim') }} e
left join {{ source('gold', 'muscles_bridge') }} m
    on e.exercise_id = m.exercise_id
left join {{ source('gold', 'instructions_bridge') }} i
    on e.exercise_id = i.exercise_id
