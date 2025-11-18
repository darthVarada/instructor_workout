from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Cria uma SparkSession
spark = SparkSession.builder \
    .appName("ExemploImport") \
    .getOrCreate()

# Exemplo de uso da função expr
# df é o seu DataFrame
# display(df.select("Count", expr("lower(County) as little_name")))
df = spark.createDataFrame(
    [
        (1, "Alice", 29),
        (2, "Bob", 31),
        (3, "Charlie", 25)
    ],
    ["id", "name", "age"]
)
df.select("id", expr("lower(name) as lower_name"), "age").show()
# Encerra a SparkSession
spark.stop()
