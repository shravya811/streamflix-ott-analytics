"""
StreamFlix OTT Analytics — Phase 2: Pandas EDA
================================================
Loads the 6 CSVs, cleans and merges them, adds calculated columns, runs
exploratory analysis, prints summary reports, and saves charts as PNGs.

Run:
    pip install pandas matplotlib
    python phase2_pandas_eda.py

Expects a `data/` folder next to this script containing:
    movies.csv, users.csv, subscription_plans.csv,
    subscriptions.csv, watch_history.csv, ratings.csv

Outputs a `charts/` folder with 9 PNG charts plus console summary reports.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

pd.set_option("display.width", 120)
pd.set_option("display.max_columns", 20)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CHART_DIR = os.path.join(BASE_DIR, "charts")
os.makedirs(CHART_DIR, exist_ok=True)

plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["axes.titleweight"] = "bold"


# ============================================================================
# 1. LOAD
# ============================================================================
print("=" * 70)
print("1. LOADING DATA")
print("=" * 70)

movies = pd.read_csv(os.path.join(DATA_DIR, "movies.csv"), parse_dates=["date_added"])
users = pd.read_csv(os.path.join(DATA_DIR, "users.csv"), parse_dates=["signup_date"])
plans = pd.read_csv(os.path.join(DATA_DIR, "subscription_plans.csv"))
subs = pd.read_csv(
    os.path.join(DATA_DIR, "subscriptions.csv"),
    parse_dates=["start_date", "end_date"]
)
watch = pd.read_csv(os.path.join(DATA_DIR, "watch_history.csv"), parse_dates=["watch_date"])
ratings = pd.read_csv(os.path.join(DATA_DIR, "ratings.csv"), parse_dates=["rating_date"])

for name, df in [("movies", movies), ("users", users), ("plans", plans),
                  ("subscriptions", subs), ("watch_history", watch), ("ratings", ratings)]:
    print(f"  {name:15s}: {df.shape[0]:>7,} rows x {df.shape[1]} cols")


# ============================================================================
# 2. CLEAN
# ============================================================================
print("\n" + "=" * 70)
print("2. CLEANING")
print("=" * 70)

# Movies: fill missing text fields, drop exact dupes
before = len(movies)
movies = movies.drop_duplicates(subset=["show_id"])
movies["director"] = movies["director"].fillna("Unknown")
movies["primary_country"] = movies["primary_country"].fillna("Unknown")
print(f"  movies: {before} -> {len(movies)} rows after dedup")

# Watch history: drop rows with non-positive duration, clip completion_pct to [0,100]
before = len(watch)
watch = watch[watch["watch_duration_minutes"] > 0].copy()
watch["completion_pct"] = watch["completion_pct"].clip(0, 100)
print(f"  watch_history: {before} -> {len(watch)} rows after removing invalid durations")

# Ratings: keep only valid 1-5
before = len(ratings)
ratings = ratings[ratings["rating"].between(1, 5)]
print(f"  ratings: {before} -> {len(ratings)} rows after range filter")

# Subscriptions: standardize status casing
subs["status"] = subs["status"].str.strip().str.title()

print("\nNull counts (top offenders):")
print(movies.isnull().sum().sort_values(ascending=False).head(5))


# ============================================================================
# 3. MERGE
# ============================================================================
print("\n" + "=" * 70)
print("3. MERGING")
print("=" * 70)

watch_full = (
    watch
    .merge(movies[["show_id", "title", "type", "primary_genre", "release_year"]], on="show_id", how="left")
    .merge(users[["user_id", "city", "country", "age", "signup_date"]], on="user_id", how="left")
)

subs_full = (
    subs
    .merge(plans, on="plan_id", how="left")
    .merge(users[["user_id", "city", "country"]], on="user_id", how="left")
)

ratings_full = ratings.merge(movies[["show_id", "title", "primary_genre"]], on="show_id", how="left")

print(f"  watch_full  : {watch_full.shape}")
print(f"  subs_full   : {subs_full.shape}")
print(f"  ratings_full: {ratings_full.shape}")


# ============================================================================
# 4. CALCULATED COLUMNS
# ============================================================================
print("\n" + "=" * 70)
print("4. CALCULATED COLUMNS")
print("=" * 70)

watch_full["watch_month"] = watch_full["watch_date"].dt.to_period("M").astype(str)
watch_full["watch_year"] = watch_full["watch_date"].dt.year
watch_full["is_binge"] = watch_full["completion_pct"] >= 90

subs_full["is_active"] = subs_full["status"] == "Active"
subs_full["tenure_days"] = (
    subs_full["end_date"].fillna(pd.Timestamp.today()) - subs_full["start_date"]
).dt.days

users["age_group"] = pd.cut(
    users["age"], bins=[0, 18, 30, 45, 100],
    labels=["Teen", "Young Adult", "Adult", "Senior"]
)

movies["content_age_years"] = pd.Timestamp.today().year - movies["release_year"]

print("  Added: watch_month, watch_year, is_binge, is_active, tenure_days, age_group, content_age_years")


# ============================================================================
# 5. EXPLORATORY DATA ANALYSIS + SUMMARY REPORTS
# ============================================================================
print("\n" + "=" * 70)
print("5. EDA — SUMMARY REPORTS")
print("=" * 70)

print("\n--- Top 10 Most-Watched Titles ---")
top_titles = (
    watch_full.groupby("title")
    .agg(total_views=("watch_id", "count"), total_minutes=("watch_duration_minutes", "sum"))
    .sort_values("total_views", ascending=False)
    .head(10)
)
print(top_titles)

print("\n--- Most Popular Genres by Watch Time ---")
popular_genres = (
    watch_full.groupby("primary_genre")
    .agg(view_count=("watch_id", "count"), total_minutes=("watch_duration_minutes", "sum"))
    .sort_values("total_minutes", ascending=False)
)
print(popular_genres.head(10))

print("\n--- Most Active Cities ---")
active_cities = (
    watch_full.groupby(["city", "country"])
    .agg(watch_events=("watch_id", "count"), active_users=("user_id", "nunique"))
    .sort_values("watch_events", ascending=False)
    .head(10)
)
print(active_cities)

print("\n--- Revenue by Subscription Plan ---")
revenue_by_plan = (
    subs_full.groupby("plan_name")
    .agg(subscription_count=("subscription_id", "count"), total_revenue=("monthly_revenue", "sum"))
    .sort_values("total_revenue", ascending=False)
)
print(revenue_by_plan)

print("\n--- Device Usage ---")
device_usage = (
    watch_full.groupby("device")
    .agg(sessions=("watch_id", "count"), total_minutes=("watch_duration_minutes", "sum"),
         avg_minutes=("watch_duration_minutes", "mean"))
    .sort_values("sessions", ascending=False)
)
print(device_usage.round(1))

print("\n--- Top 10 Rated Movies (min 5 ratings) ---")
rating_stats = (
    ratings_full.groupby(["show_id", "title"])
    .agg(num_ratings=("rating", "count"), avg_rating=("rating", "mean"))
)
top_rated = rating_stats[rating_stats["num_ratings"] >= 5].sort_values(
    ["avg_rating", "num_ratings"], ascending=False
).head(10)
print(top_rated.round(2))

print("\n--- Monthly Viewing Trend (last 12 months) ---")
monthly_trend = (
    watch_full.groupby("watch_month")
    .agg(total_views=("watch_id", "count"), total_minutes=("watch_duration_minutes", "sum"),
         unique_viewers=("user_id", "nunique"))
    .sort_index()
)
print(monthly_trend.tail(12))

print("\n--- Churn-Risk Users (High = no watch in 60+ days, of active subs) ---")
last_watch = watch_full.groupby("user_id")["watch_date"].max().rename("last_watch_date")
active_users = subs_full[subs_full["is_active"]]["user_id"].unique()
churn = pd.DataFrame({"user_id": active_users}).merge(last_watch, on="user_id", how="left")
churn["days_since_last_watch"] = (pd.Timestamp.today() - churn["last_watch_date"]).dt.days
churn["churn_risk"] = pd.cut(
    churn["days_since_last_watch"], bins=[-1, 30, 60, 100000],
    labels=["Low", "Medium", "High"]
)
print(churn["churn_risk"].value_counts())

print("\n--- Average Watch Time per User ---")
avg_watch_per_user = watch_full.groupby("user_id")["watch_duration_minutes"].sum()
print(f"  Mean total watch minutes/user : {avg_watch_per_user.mean():.1f}")
print(f"  Median total watch minutes/user: {avg_watch_per_user.median():.1f}")


# ============================================================================
# 6. VISUALIZATIONS
# ============================================================================
print("\n" + "=" * 70)
print("6. GENERATING CHARTS")
print("=" * 70)

def save(fig, name):
    path = os.path.join(CHART_DIR, name)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  -> saved {name}")

# 6.1 Top 10 most-watched titles
fig, ax = plt.subplots()
top_titles["total_views"].sort_values().plot(kind="barh", ax=ax, color="#E50914")
ax.set_title("Top 10 Most-Watched Titles")
ax.set_xlabel("Total Views")
save(fig, "01_top_watched_titles.png")

# 6.2 Popular genres
fig, ax = plt.subplots()
popular_genres.head(10)["total_minutes"].sort_values().plot(kind="barh", ax=ax, color="#221F1F")
ax.set_title("Top 10 Genres by Total Watch Minutes")
ax.set_xlabel("Total Minutes")
save(fig, "02_popular_genres.png")

# 6.3 Revenue by plan (pie)
fig, ax = plt.subplots()
ax.pie(revenue_by_plan["total_revenue"], labels=revenue_by_plan.index, autopct="%1.1f%%",
       colors=["#E50914", "#B81D24", "#221F1F"])
ax.set_title("Revenue Share by Subscription Plan")
save(fig, "03_revenue_by_plan.png")

# 6.4 Device usage
fig, ax = plt.subplots()
device_usage["sessions"].plot(kind="bar", ax=ax, color="#E50914")
ax.set_title("Watch Sessions by Device")
ax.set_ylabel("Sessions")
plt.xticks(rotation=0)
save(fig, "04_device_usage.png")

# 6.5 Monthly viewing trend
fig, ax = plt.subplots()
monthly_trend["total_views"].plot(kind="line", ax=ax, marker="o", color="#E50914")
ax.set_title("Monthly Viewing Trend")
ax.set_ylabel("Total Views")
ax.set_xlabel("Month")
plt.xticks(rotation=90, fontsize=7)
save(fig, "05_monthly_trend.png")

# 6.6 Active cities
fig, ax = plt.subplots()
active_cities["watch_events"].sort_values().plot(kind="barh", ax=ax, color="#221F1F")
ax.set_title("Top 10 Active Cities by Watch Events")
ax.set_xlabel("Watch Events")
save(fig, "06_active_cities.png")

# 6.7 Churn risk distribution
fig, ax = plt.subplots()
churn["churn_risk"].value_counts().reindex(["Low", "Medium", "High"]).plot(
    kind="bar", ax=ax, color=["#2ecc71", "#f39c12", "#e74c3c"]
)
ax.set_title("Churn Risk Distribution (Active Subscribers)")
ax.set_ylabel("User Count")
plt.xticks(rotation=0)
save(fig, "07_churn_risk.png")

# 6.8 Age group distribution
fig, ax = plt.subplots()
users["age_group"].value_counts().reindex(["Teen", "Young Adult", "Adult", "Senior"]).plot(
    kind="bar", ax=ax, color="#B81D24"
)
ax.set_title("User Distribution by Age Group")
ax.set_ylabel("User Count")
plt.xticks(rotation=0)
save(fig, "08_age_groups.png")

# 6.9 Content type split (Movie vs TV Show) among watched content
fig, ax = plt.subplots()
watch_full["type"].value_counts().plot(kind="pie", ax=ax, autopct="%1.1f%%",
                                        colors=["#E50914", "#221F1F"])
ax.set_title("Watch Events: Movies vs TV Shows")
ax.set_ylabel("")
save(fig, "09_movie_vs_tv_watch_share.png")

print(f"\nAll charts saved to: {CHART_DIR}")
print("\nPhase 2 EDA complete.")
