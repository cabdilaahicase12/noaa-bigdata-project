from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("NOAA_LinearRegression") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.parquet("/user/hive/warehouse/weather.db/noaa_observations")

df = df.filter(df.element == "TMAX")

assembler = VectorAssembler(
    inputCols=["year", "month", "day"],
    outputCol="features"
)

data = assembler.transform(df)

train, test = data.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(
    featuresCol="features",
    labelCol="value"
)

model = lr.fit(train)

predictions = model.transform(test)

rmse = RegressionEvaluator(
    labelCol="value",
    predictionCol="prediction",
    metricName="rmse"
).evaluate(predictions)

r2 = RegressionEvaluator(
    labelCol="value",
    predictionCol="prediction",
    metricName="r2"
).evaluate(predictions)

print("RMSE =", rmse)
print("R2 =", r2)

spark.stop()
