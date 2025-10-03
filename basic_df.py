from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("basic_df").getOrCreate()
df = spark.read.option("header", "true").csv("employees.csv")

# show name and salary
df.select("name", "salary").show()

# show employees who work in engineering
df.filter(df.department == "Engineering").show()

# Add a column for the bonus at 10% of salary
df_with_bonus = df.withColumn("bonus", df.salary * 0.1)
df_with_bonus.show()

# Rename the bonus column to annual bonus
df_renamed = df_with_bonus.withColumnRenamed("bonus", "annual_bonus")
df_renamed.show()

spark.stop()
