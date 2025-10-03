from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg

spark = SparkSession.builder.appName("WorkingJSON").getOrCreate()

df = spark.read.option("header", "true").csv("employees.csv")

# register the df as a temp view
df.createOrReplaceTempView("employees_table")

# run query
result = spark.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM employees_table
    GROUP BY department
    HAVING avg_salary > 70000
""")

result.show()

spark.stop()
