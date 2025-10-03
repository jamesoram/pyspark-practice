from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col
spark = SparkSession.builder.appName("word_count").getOrCreate()

text_df = spark.read.text("shakespeare.txt")

# split into words, make lowercase and filter non-empty
words_df = text_df.select(explode(split(lower(col("value")), "\\W+")).alias("word"))
words_df = words_df.filter(col("word") != "")

# count word frequency
word_counts = words_df.groupBy("word").count().orderBy("count", ascending=False)
word_counts.show(10)

spark.stop()
