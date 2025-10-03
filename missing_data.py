from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MissingData").getOrCreate()
df = spark.read.option("header", "true").csv("employees_missing.csv")

# Show rows with null values in name or salary
df.filter(df.name.isNull()).show()
df.filter(df.salary.isNull()).show()

# Drop the rows that contain null values
df_clean = df.dropna()
df_clean.show()

# fill in nulls with appropriare values
df_filled = df.fillna({"salary" : 0, "name" : "Unknown"})
df_filled.show()

spark.stop()
