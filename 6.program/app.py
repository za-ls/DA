#LoadingData: First, load the data into Spark RDDs or DataFrames.
from pyspark.sql import SparkSession
# Initialize Spark session
spark = SparkSession.builder.appName("MovieRatingsAnalysis").getOrCreate()
# Load datasets
movies_df = spark.read.csv("add file", header=True, inferSchema=True)
ratings_df = spark.read.csv("add csv", header=True, inferSchema=True)
# Create RDDs
movies_rdd = movies_df.rdd 
ratings_rdd = ratings_df.rdd

# (a) Find the Movie with the Lowest Average Rating Using RDD.
# Compute average ratings
avg_ratings_rdd = ratings_rdd.map(lambda x: (x['movieId'], (x['rating'], 1))) \
.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
.mapValues(lambda x: x[0] / x[1])
# Find the movie with the lowest average rating
lowest_avg_rating = avg_ratings_rdd.sortBy(lambda x: x[1]).first() 
print(f"Movie with the lowest average rating: {lowest_avg_rating}")


#(b) Identify Users Who Have Rated the Most Movies.
# Compute number of ratings per user
user_ratings_count = ratings_rdd.map(lambda x: (x['userId'], 1)) \
.reduceByKey(lambda x, y: x + y) \
.sortBy(lambda x: x[1], ascending=False)
# Get top users
top_users = user_ratings_count.take(10)
print(f"Top users by number of ratings: {top_users}")

# (c) Explore the Distribution of Ratings Over Time.
from pyspark.sql.functions import from_unixtime, year, month
# Convert timestamp to date and extract year and month
ratings_df = ratings_df.withColumn("year", year(from_unixtime(ratings_df['timestamp']))) \
.withColumn("month", month(from_unixtime(ratings_df['timestamp']))) 
# Group by year and month to get rating counts
ratings_over_time = ratings_df.groupBy("year", "month").count().orderBy("year", "month")
# Show distribution 
ratings_over_time.show()

# (d) Find the Highest-Rated Movies with a Minimum Number of Ratings.
# Compute average ratings and count ratings per movie
movie_ratings_stats = ratings_rdd.map(lambda x: (x['movieId'], (x['rating'], 1))) \
.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
.mapValues(lambda x: (x[0] / x[1], x[1])) 
# Filter movies with a minimum number of ratings
min_ratings = 100
qualified_movies = movie_ratings_stats.filter(lambda x: x[1][1] >= min_ratings) 
# Find the highest-rated movies
highest_rated_movies = qualified_movies.sortBy(lambda x: x[1][0], ascending=False).take(10) 
print(f"Highest-rated movies with at least {min_ratings} ratings: {highest_rated_movies}")
