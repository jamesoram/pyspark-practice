from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg

spark = SparkSession.builder.appName("WorkingJSON").getOrCreate()

df = spark.read.json("students.json")

df.printSchema()

# Access nested array
df.select("name", "grades").show()

# Show average grades using explode
df_exploded = df.select("name", explode("grades").alias("grade"))
df_exploded.groupBy().avg("grade").show()

spark.stop()
