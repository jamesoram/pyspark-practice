from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("FirstSession").getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the session
spark.stop()
