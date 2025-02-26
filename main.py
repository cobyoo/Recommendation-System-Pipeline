from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import explode, col
import random

spark = SparkSession.builder.appName("RecommendationSystem").getOrCreate()

data = [
    (0, 0, 4.0), (0, 1, 2.0), (0, 2, 5.0),
    (1, 0, 5.0), (1, 1, 3.0), (1, 2, 4.0),
    (2, 0, 2.0), (2, 1, 4.0), (2, 2, 3.0)
]
columns = ["userId", "movieId", "rating"]
ratings = spark.createDataFrame(data, columns)

als = ALS(
    userCol="userId", itemCol="movieId", ratingCol="rating",
    coldStartStrategy="drop", nonnegative=True
)

model = als.fit(ratings)

userRecs = model.recommendForAllUsers(3)

recommendations = userRecs.select("userId", explode("recommendations").alias("rec")) \
    .select("userId", col("rec.movieId").alias("movieId"), col("rec.rating").alias("predictedRating"))

recommendations.show()

spark.stop()
