from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# Schema dos workouts baseado no CSV workout_data.csv
WORKOUT_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("start_time", StringType(), True),   # depois podemos converter para timestamp
        StructField("end_time", StringType(), True),
        StructField("description", StringType(), True),
        StructField("exercise_title", StringType(), True),
        StructField("superset_id", IntegerType(), True),
        StructField("exercise_notes", StringType(), True),
        StructField("set_index", IntegerType(), True),
        StructField("set_type", StringType(), True),
        StructField("weight_kg", DoubleType(), True),
        StructField("reps", DoubleType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("rpe", DoubleType(), True),
    ]
)
