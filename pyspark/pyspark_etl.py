"""
StreamFlix OTT Analytics — Phase 3: PySpark ETL Pipeline
==========================================================
Builds a scalable ETL pipeline over the StreamFlix datasets:
  - movies.csv              (curated from Kaggle netflix_titles.csv)
  - users.csv
  - subscription_plans.csv
  - subscriptions.csv       (fact: revenue + churn source)
  - watch_history.csv       (fact: engagement source)
  - ratings.csv              (fact: ratings source)

What it does:
  1. Reads raw CSVs with explicit schemas
  2. Cleans / transforms each dataset (nulls, types, dedup)
  3. Joins into a unified analytical model
  4. Computes KPIs using Spark SQL, DataFrame API, and window functions
  5. Caches reused DataFrames
  6. Writes curated, partitioned Parquet datasets to /spark_output

Run locally:
    pip install pyspark
    python pyspark_etl.py

Run via spark-submit (recommended for larger data):
    spark-submit pyspark_etl.py
"""

import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

# --------------------------------------------------------------------------
# 0. PATHS
# --------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUT_DIR = os.path.join(BASE_DIR, "spark_output")

# --------------------------------------------------------------------------
# 1. SPARK SESSION
# --------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("StreamFlix-OTT-Analytics-ETL")
    .config("spark.sql.shuffle.partitions", "8")   # tune down for local/small data
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------------------------------
# 2. EXPLICIT SCHEMAS (avoids slow/incorrect schema inference)
# --------------------------------------------------------------------------
movies_schema = StructType([
    StructField("show_id", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("director", StringType()),
    StructField("cast", StringType()),
    StructField("primary_country", StringType()),
    StructField("all_countries", StringType()),
    StructField("date_added", DateType()),
    StructField("release_year", IntegerType()),
    StructField("rating", StringType()),
    StructField("duration_value", IntegerType()),
    StructField("duration_unit", StringType()),
    StructField("primary_genre", StringType()),
    StructField("all_genres", StringType()),
    StructField("description", StringType()),
])

users_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("signup_date", DateType()),
])

plans_schema = StructType([
    StructField("plan_id", IntegerType()),
    StructField("plan_name", StringType()),
    StructField("monthly_price", DoubleType()),
    StructField("max_streams", IntegerType()),
    StructField("video_quality", StringType()),
])

subs_schema = StructType([
    StructField("subscription_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("plan_id", IntegerType()),
    StructField("start_date", DateType()),
    StructField("end_date", DateType()),
    StructField("status", StringType()),
    StructField("monthly_revenue", DoubleType()),
])

watch_schema = StructType([
    StructField("watch_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("show_id", StringType()),
    StructField("device", StringType()),
    StructField("watch_date", DateType()),
    StructField("watch_duration_minutes", DoubleType()),
    StructField("completion_pct", DoubleType()),
])

ratings_schema = StructType([
    StructField("rating_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("show_id", StringType()),
    StructField("rating", IntegerType()),
    StructField("rating_date", DateType()),
])

# --------------------------------------------------------------------------
# 3. EXTRACT — read raw CSVs
# --------------------------------------------------------------------------
def read_csv(name, schema):
    return (
        spark.read
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(os.path.join(DATA_DIR, name))
    )

raw_movies = read_csv("movies.csv", movies_schema)
raw_users = read_csv("users.csv", users_schema)
raw_plans = read_csv("subscription_plans.csv", plans_schema)
raw_subs = read_csv("subscriptions.csv", subs_schema)
raw_watch = read_csv("watch_history.csv", watch_schema)
raw_ratings = read_csv("ratings.csv", ratings_schema)

# --------------------------------------------------------------------------
# 4. TRANSFORM — clean each dataset
# --------------------------------------------------------------------------

# 4a. Movies: drop dup show_ids, trim strings, flag content age
dim_movies = (
    raw_movies
    .dropDuplicates(["show_id"])
    .withColumn("title", F.trim(F.col("title")))
    .withColumn("primary_genre", F.trim(F.col("primary_genre")))
    .withColumn(
        "content_age_years",
        F.year(F.current_date()) - F.col("release_year")
    )
    .withColumn(
        "is_recent_release",
        F.when(F.col("content_age_years") <= 3, True).otherwise(False)
    )
)

# 4b. Users: clean, bucket age groups
dim_users = (
    raw_users
    .dropDuplicates(["user_id"])
    .withColumn(
        "age_group",
        F.when(F.col("age") < 18, "Teen")
         .when(F.col("age") < 30, "Young Adult")
         .when(F.col("age") < 45, "Adult")
         .otherwise("Senior")
    )
)

# 4c. Subscriptions: standardize status, compute active flag & tenure
fact_subscriptions = (
    raw_subs
    .withColumn("is_active", F.when(F.col("status") == "Active", True).otherwise(False))
    .withColumn(
        "tenure_days",
        F.datediff(F.coalesce(F.col("end_date"), F.current_date()), F.col("start_date"))
    )
)

# 4d. Watch history: filter invalid rows, clip completion_pct
fact_watch = (
    raw_watch
    .filter(F.col("watch_duration_minutes") >= 0)
    .withColumn(
        "completion_pct",
        F.when(F.col("completion_pct") > 100, 100.0).otherwise(F.col("completion_pct"))
    )
    .withColumn(
        "watch_month",
        F.date_format(F.col("watch_date"), "yyyy-MM")
    )
)

# 4e. Ratings: keep valid 1-5 range only
fact_ratings = raw_ratings.filter((F.col("rating") >= 1) & (F.col("rating") <= 5))

# Cache frequently reused DataFrames
dim_movies.cache()
dim_users.cache()
fact_watch.cache()
fact_subscriptions.cache()
dim_movies.count()      # materialize cache
fact_watch.count()

# --------------------------------------------------------------------------
# 5. JOINS — build unified analytical views
# --------------------------------------------------------------------------

# Watch history enriched with movie + user info
watch_enriched = (
    fact_watch
    .join(dim_movies.select("show_id", "title", "type", "primary_genre", "release_year"), "show_id", "left")
    .join(dim_users.select("user_id", "city", "country", "age_group"), "user_id", "left")
)

# Subscriptions enriched with plan info
subs_enriched = (
    fact_subscriptions
    .join(raw_plans, "plan_id", "left")
    .join(dim_users.select("user_id", "city", "country"), "user_id", "left")
)

# Ratings enriched with movie info
ratings_enriched = (
    fact_ratings
    .join(dim_movies.select("show_id", "title", "primary_genre"), "show_id", "left")
)

# --------------------------------------------------------------------------
# 6. SPARK SQL + WINDOW FUNCTIONS — business KPIs
# --------------------------------------------------------------------------
watch_enriched.createOrReplaceTempView("watch_enriched")
subs_enriched.createOrReplaceTempView("subs_enriched")
ratings_enriched.createOrReplaceTempView("ratings_enriched")
dim_movies.createOrReplaceTempView("dim_movies")

# 6a. Top 10 most-watched titles (Spark SQL)
top_watched_titles = spark.sql("""
    SELECT title, type, primary_genre,
           COUNT(*) AS total_views,
           ROUND(SUM(watch_duration_minutes), 1) AS total_watch_minutes
    FROM watch_enriched
    GROUP BY title, type, primary_genre
    ORDER BY total_views DESC
    LIMIT 10
""")

# 6b. Popular genres by total watch time
popular_genres = spark.sql("""
    SELECT primary_genre,
           COUNT(*) AS view_count,
           ROUND(SUM(watch_duration_minutes), 1) AS total_minutes
    FROM watch_enriched
    GROUP BY primary_genre
    ORDER BY total_minutes DESC
""")

# 6c. Active cities by number of watch events
active_cities = spark.sql("""
    SELECT city, country, COUNT(*) AS watch_events, COUNT(DISTINCT user_id) AS active_users
    FROM watch_enriched
    GROUP BY city, country
    ORDER BY watch_events DESC
""")

# 6d. Revenue by subscription plan
revenue_by_plan = spark.sql("""
    SELECT plan_name,
           COUNT(*) AS subscription_count,
           ROUND(SUM(monthly_revenue), 2) AS total_revenue,
           ROUND(AVG(monthly_revenue), 2) AS avg_revenue_per_sub
    FROM subs_enriched
    GROUP BY plan_name
    ORDER BY total_revenue DESC
""")

# 6e. Device usage distribution
device_usage = spark.sql("""
    SELECT device, COUNT(*) AS sessions,
           ROUND(SUM(watch_duration_minutes), 1) AS total_minutes,
           ROUND(AVG(watch_duration_minutes), 1) AS avg_minutes_per_session
    FROM watch_enriched
    GROUP BY device
    ORDER BY sessions DESC
""")

# 6f. Top-rated movies (min 5 ratings) — DataFrame API + window function
rating_stats = (
    ratings_enriched.groupBy("show_id", "title")
    .agg(
        F.count("*").alias("num_ratings"),
        F.round(F.avg("rating"), 2).alias("avg_rating")
    )
    .filter(F.col("num_ratings") >= 5)
)
top_rated_window = Window.orderBy(F.desc("avg_rating"), F.desc("num_ratings"))
top_rated_movies = (
    rating_stats
    .withColumn("rank", F.row_number().over(top_rated_window))
    .filter(F.col("rank") <= 20)
)

# 6g. Monthly viewing trend
monthly_trend = spark.sql("""
    SELECT watch_month, COUNT(*) AS total_views,
           ROUND(SUM(watch_duration_minutes), 1) AS total_minutes,
           COUNT(DISTINCT user_id) AS unique_viewers
    FROM watch_enriched
    GROUP BY watch_month
    ORDER BY watch_month
""")

# 6h. Per-user engagement + churn-risk flag (window functions)
user_last_watch = (
    fact_watch.groupBy("user_id")
    .agg(
        F.max("watch_date").alias("last_watch_date"),
        F.count("*").alias("total_sessions"),
        F.round(F.sum("watch_duration_minutes"), 1).alias("total_watch_minutes")
    )
)
user_engagement = (
    user_last_watch
    .join(fact_subscriptions.filter(F.col("is_active") == True).select("user_id", "plan_id", "start_date"), "user_id", "left")
    .withColumn("days_since_last_watch", F.datediff(F.current_date(), F.col("last_watch_date")))
    .withColumn(
        "churn_risk",
        F.when(F.col("days_since_last_watch") > 60, "High")
         .when(F.col("days_since_last_watch") > 30, "Medium")
         .otherwise("Low")
    )
)

# 6i. Recommendation-ready dataset: top 5 genres watched per user (window function)
genre_counts = (
    watch_enriched.groupBy("user_id", "primary_genre")
    .agg(F.count("*").alias("genre_view_count"))
)
genre_rank_window = Window.partitionBy("user_id").orderBy(F.desc("genre_view_count"))
user_top_genres = (
    genre_counts
    .withColumn("genre_rank", F.row_number().over(genre_rank_window))
    .filter(F.col("genre_rank") <= 5)
)

# --------------------------------------------------------------------------
# 7. LOAD — write curated Parquet outputs (partitioned where useful)
# --------------------------------------------------------------------------
def write_parquet(df, name, partition_cols=None):
    path = os.path.join(OUT_DIR, name)
    writer = df.coalesce(4).write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    print(f"  -> wrote {name} ({df.count()} rows){' partitioned by ' + str(partition_cols) if partition_cols else ''}")

print("Writing curated Parquet datasets...")
write_parquet(dim_movies, "dim_movies", partition_cols=["type"])
write_parquet(dim_users, "dim_users")
write_parquet(fact_subscriptions, "fact_subscriptions", partition_cols=["status"])
write_parquet(fact_watch.drop("watch_month"), "fact_watch_history", partition_cols=None)
write_parquet(watch_enriched.withColumn("watch_year", F.year("watch_date")), "watch_enriched", partition_cols=["watch_year"])
write_parquet(top_watched_titles, "kpi_top_watched_titles")
write_parquet(popular_genres, "kpi_popular_genres")
write_parquet(active_cities, "kpi_active_cities")
write_parquet(revenue_by_plan, "kpi_revenue_by_plan")
write_parquet(device_usage, "kpi_device_usage")
write_parquet(top_rated_movies, "kpi_top_rated_movies")
write_parquet(monthly_trend, "kpi_monthly_trend")
write_parquet(user_engagement, "kpi_user_engagement_churn")
write_parquet(user_top_genres, "kpi_user_recommendation_genres")

# --------------------------------------------------------------------------
# 8. QUICK PREVIEW IN CONSOLE
# --------------------------------------------------------------------------
print("\n=== TOP 10 MOST-WATCHED TITLES ===")
top_watched_titles.show(10, truncate=False)

print("\n=== REVENUE BY PLAN ===")
revenue_by_plan.show()

print("\n=== DEVICE USAGE ===")
device_usage.show()

print("\n=== TOP 10 RATED MOVIES (min 5 ratings) ===")
top_rated_movies.orderBy("rank").show(10, truncate=False)

print("\n=== MONTHLY VIEWING TREND (last 12 months) ===")
monthly_trend.orderBy(F.desc("watch_month")).show(12)

print("\n=== CHURN RISK DISTRIBUTION ===")
user_engagement.groupBy("churn_risk").count().show()

spark.stop()
print("\nETL pipeline complete. Curated Parquet output in ./spark_output/")
