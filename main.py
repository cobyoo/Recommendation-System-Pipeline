from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import explode, col
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder.appName("AdvancedRecommendationSystem").getOrCreate()

data = [
    (0, 0, 4.0), (0, 1, 2.0), (0, 2, 5.0),
    (1, 0, 5.0), (1, 1, 3.0), (1, 2, 4.0),
    (2, 0, 2.0), (2, 1, 4.0), (2, 2, 3.0),
    (3, 0, 3.0), (3, 1, 4.0), (3, 2, 5.0)
]
columns = ["userId", "movieId", "rating"]
ratings = spark.createDataFrame(data, columns) \
    .withColumn("userId", col("userId").cast(IntegerType())) \
    .withColumn("movieId", col("movieId").cast(IntegerType())) \
    .withColumn("rating", col("rating").cast(FloatType()))

train, test = ratings.randomSplit([0.8, 0.2], seed=42)

als = ALS(
    userCol="userId", itemCol="movieId", ratingCol="rating",
    coldStartStrategy="drop", nonnegative=True, 
    implicitPrefs=False, rank=10, maxIter=10, regParam=0.1
)

model = als.fit(train)

predictions = model.transform(test)
evaluator = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction"
)
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse:.4f}")

userRecs = model.recommendForAllUsers(3)

userSubset = spark.createDataFrame([(1,)], ["userId"])
userSpecificRecs = model.recommendForUserSubset(userSubset, 3)

userRecs.select("userId", explode("recommendations").alias("rec")) \
    .select("userId", col("rec.movieId").alias("movieId"), col("rec.rating").alias("predictedRating")) \
    .show()

userSpecificRecs.select("userId", explode("recommendations").alias("rec")) \
    .select("userId", col("rec.movieId").alias("movieId"), col("rec.rating").alias("predictedRating")) \
    .show()

spark.stop()
