from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleModel").getOrCreate()

# sample data: features (hours studied), label (exam score)
data = [(1, 50), (2, 60), (3, 70), (4, 80), (5, 90)]
df = spark.createDataFrame(data, ["hours", "score"])

# assemble features into a vector
assembler = VectorAssembler(inputCols=["hours"], outputCol="features")
df_assembled = assembler.transform(df)

# train the model
lr = LinearRegression(featuresCol="features", labelCol="score")
model = lr.fit(df_assembled)

# make predictions
predictions = model.transform(df_assembled)
predictions.select("hours", "score", "prediction").show()

spark.stop()
