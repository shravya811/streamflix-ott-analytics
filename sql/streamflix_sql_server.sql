/* ============================================================================
   StreamFlix OTT Analytics — Phase 1: SQL Server (SSMS) Script
   ============================================================================
   Run this in SQL Server Management Studio, connected to your VM's SQL Server
   instance. Execute top to bottom, OR run section by section (they're marked).

   SECTIONS:
     1. Create database
     2. Create normalized tables (DDL) with PK/FK relationships
     3. Load data (BULK INSERT — requires CSVs to be on a path the SQL Server
        SERVICE ACCOUNT can read; see README for the alternate GUI method if
        BULK INSERT gives permission errors)
     4. Verify row counts
     5. Business analytics: joins, aggregations, CTEs, window functions, views
   ============================================================================ */

-- ============================================================================
-- 1. CREATE DATABASE
-- ============================================================================
IF DB_ID('StreamFlix') IS NULL
BEGIN
    CREATE DATABASE StreamFlix;
END
GO

USE StreamFlix;
GO

-- ============================================================================
-- 2. CREATE NORMALIZED TABLES
-- ============================================================================

IF OBJECT_ID('dbo.Ratings', 'U') IS NOT NULL DROP TABLE dbo.Ratings;
IF OBJECT_ID('dbo.WatchHistory', 'U') IS NOT NULL DROP TABLE dbo.WatchHistory;
IF OBJECT_ID('dbo.Subscriptions', 'U') IS NOT NULL DROP TABLE dbo.Subscriptions;
IF OBJECT_ID('dbo.Users', 'U') IS NOT NULL DROP TABLE dbo.Users;
IF OBJECT_ID('dbo.SubscriptionPlans', 'U') IS NOT NULL DROP TABLE dbo.SubscriptionPlans;
IF OBJECT_ID('dbo.Movies', 'U') IS NOT NULL DROP TABLE dbo.Movies;
GO

CREATE TABLE dbo.Movies (
    show_id             VARCHAR(10)     NOT NULL PRIMARY KEY,
    type                VARCHAR(10)     NOT NULL,          -- 'Movie' or 'TV Show'
    title               NVARCHAR(200)   NOT NULL,
    director            NVARCHAR(300),
    cast_members        NVARCHAR(1000),   -- named cast_members: "cast" is a reserved T-SQL keyword
    primary_country     NVARCHAR(50),
    all_countries       NVARCHAR(300),
    date_added          DATE,
    release_year        SMALLINT        NOT NULL,
    rating              VARCHAR(10),
    duration_value      SMALLINT,
    duration_unit       VARCHAR(10),                        -- 'min' or 'Season'
    primary_genre       NVARCHAR(50),
    all_genres          NVARCHAR(200),
    description         NVARCHAR(500)
);
GO

CREATE TABLE dbo.Users (
    user_id             INT             NOT NULL PRIMARY KEY,
    name                NVARCHAR(100)   NOT NULL,
    email               VARCHAR(150)    NOT NULL,
    gender              VARCHAR(10),
    age                 TINYINT,
    city                NVARCHAR(50),
    country             NVARCHAR(50),
    signup_date         DATE            NOT NULL
);
GO

CREATE TABLE dbo.SubscriptionPlans (
    plan_id             INT             NOT NULL PRIMARY KEY,
    plan_name           VARCHAR(20)     NOT NULL,
    monthly_price       DECIMAL(8,2)    NOT NULL,
    max_streams         TINYINT,
    video_quality       VARCHAR(20)
);
GO

CREATE TABLE dbo.Subscriptions (
    subscription_id     INT             NOT NULL PRIMARY KEY,
    user_id             INT             NOT NULL,
    plan_id             INT             NOT NULL,
    start_date          DATE            NOT NULL,
    end_date            DATE            NULL,
    status              VARCHAR(20)     NOT NULL,           -- 'Active' or 'Cancelled'
    monthly_revenue     DECIMAL(8,2)    NOT NULL,
    CONSTRAINT FK_Subscriptions_Users FOREIGN KEY (user_id) REFERENCES dbo.Users(user_id),
    CONSTRAINT FK_Subscriptions_Plans FOREIGN KEY (plan_id) REFERENCES dbo.SubscriptionPlans(plan_id)
);
GO

CREATE TABLE dbo.WatchHistory (
    watch_id                INT             NOT NULL PRIMARY KEY,
    user_id                 INT             NOT NULL,
    show_id                 VARCHAR(10)     NOT NULL,
    device                  VARCHAR(20)     NOT NULL,        -- Mobile / Tablet / Laptop / Smart TV
    watch_date              DATE            NOT NULL,
    watch_duration_minutes  DECIMAL(8,1)    NOT NULL,
    completion_pct          DECIMAL(5,1)    NOT NULL,
    CONSTRAINT FK_Watch_Users FOREIGN KEY (user_id) REFERENCES dbo.Users(user_id),
    CONSTRAINT FK_Watch_Movies FOREIGN KEY (show_id) REFERENCES dbo.Movies(show_id)
);
GO

CREATE TABLE dbo.Ratings (
    rating_id           INT             NOT NULL PRIMARY KEY,
    user_id             INT             NOT NULL,
    show_id             VARCHAR(10)     NOT NULL,
    rating               TINYINT         NOT NULL CHECK (rating BETWEEN 1 AND 5),
    rating_date          DATE            NOT NULL,
    CONSTRAINT FK_Ratings_Users FOREIGN KEY (user_id) REFERENCES dbo.Users(user_id),
    CONSTRAINT FK_Ratings_Movies FOREIGN KEY (show_id) REFERENCES dbo.Movies(show_id)
);
GO

-- Helpful indexes for the analytics queries below
CREATE INDEX IX_Watch_ShowId ON dbo.WatchHistory(show_id);
CREATE INDEX IX_Watch_UserId ON dbo.WatchHistory(user_id);
CREATE INDEX IX_Watch_Date ON dbo.WatchHistory(watch_date);
CREATE INDEX IX_Ratings_ShowId ON dbo.Ratings(show_id);
CREATE INDEX IX_Subs_UserId ON dbo.Subscriptions(user_id);
GO

-- ============================================================================
-- 3. LOAD DATA — BULK INSERT
--    Edit the file path below to wherever you copied the CSVs on the VM,
--    e.g. 'C:\StreamFlixData\movies.csv'
--    NOTE: the SQL Server *service account* needs read access to that folder,
--    not just your login. If you get "Access is denied" or "Cannot bulk load",
--    use the GUI method in the README instead (Import Flat File Wizard).
-- ============================================================================

DECLARE @DataPath NVARCHAR(200) = 'C:\StreamFlixData\';  -- <-- EDIT THIS

BULK INSERT dbo.Movies
FROM 'C:\StreamFlixData\movies.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDQUOTE = '"',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',   -- UTF-8
    TABLOCK
);
GO

BULK INSERT dbo.Users
FROM 'C:\StreamFlixData\users.csv'
WITH (FORMAT='CSV', FIRSTROW=2, FIELDQUOTE='"', FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
GO

BULK INSERT dbo.SubscriptionPlans
FROM 'C:\StreamFlixData\subscription_plans.csv'
WITH (FORMAT='CSV', FIRSTROW=2, FIELDQUOTE='"', FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
GO

BULK INSERT dbo.Subscriptions
FROM 'C:\StreamFlixData\subscriptions.csv'
WITH (FORMAT='CSV', FIRSTROW=2, FIELDQUOTE='"', FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
GO

BULK INSERT dbo.WatchHistory
FROM 'C:\StreamFlixData\watch_history.csv'
WITH (FORMAT='CSV', FIRSTROW=2, FIELDQUOTE='"', FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
GO

BULK INSERT dbo.Ratings
FROM 'C:\StreamFlixData\ratings.csv'
WITH (FORMAT='CSV', FIRSTROW=2, FIELDQUOTE='"', FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
GO

-- ============================================================================
-- 4. VERIFY ROW COUNTS
-- ============================================================================
SELECT 'Movies' AS TableName, COUNT(*) AS Rows FROM dbo.Movies
UNION ALL SELECT 'Users', COUNT(*) FROM dbo.Users
UNION ALL SELECT 'SubscriptionPlans', COUNT(*) FROM dbo.SubscriptionPlans
UNION ALL SELECT 'Subscriptions', COUNT(*) FROM dbo.Subscriptions
UNION ALL SELECT 'WatchHistory', COUNT(*) FROM dbo.WatchHistory
UNION ALL SELECT 'Ratings', COUNT(*) FROM dbo.Ratings;
GO

-- Expected approx: Movies 8807 | Users 6000 | SubscriptionPlans 3
--                  Subscriptions 8065 | WatchHistory 120000 | Ratings ~34970

/* ============================================================================
   5. BUSINESS ANALYTICS — Joins, Aggregations, CTEs, Window Functions, Views
   ============================================================================ */

-- --------------------------------------------------------------------------
-- 5.1 Top 10 Most-Watched Titles (JOIN + GROUP BY)
-- --------------------------------------------------------------------------
SELECT TOP 10
    m.title,
    m.type,
    m.primary_genre,
    COUNT(*) AS total_views,
    ROUND(SUM(w.watch_duration_minutes), 1) AS total_watch_minutes
FROM dbo.WatchHistory w
JOIN dbo.Movies m ON w.show_id = m.show_id
GROUP BY m.title, m.type, m.primary_genre
ORDER BY total_views DESC;
GO

-- --------------------------------------------------------------------------
-- 5.2 Most Popular Genres by Total Watch Time
-- --------------------------------------------------------------------------
SELECT
    m.primary_genre,
    COUNT(*) AS view_count,
    ROUND(SUM(w.watch_duration_minutes), 1) AS total_minutes
FROM dbo.WatchHistory w
JOIN dbo.Movies m ON w.show_id = m.show_id
GROUP BY m.primary_genre
ORDER BY total_minutes DESC;
GO

-- --------------------------------------------------------------------------
-- 5.3 Most Active Cities
-- --------------------------------------------------------------------------
SELECT
    u.city,
    u.country,
    COUNT(*) AS watch_events,
    COUNT(DISTINCT w.user_id) AS active_users
FROM dbo.WatchHistory w
JOIN dbo.Users u ON w.user_id = u.user_id
GROUP BY u.city, u.country
ORDER BY watch_events DESC;
GO

-- --------------------------------------------------------------------------
-- 5.4 Revenue by Subscription Plan
-- --------------------------------------------------------------------------
SELECT
    sp.plan_name,
    COUNT(*) AS subscription_count,
    SUM(s.monthly_revenue) AS total_revenue,
    ROUND(AVG(s.monthly_revenue), 2) AS avg_revenue_per_sub
FROM dbo.Subscriptions s
JOIN dbo.SubscriptionPlans sp ON s.plan_id = sp.plan_id
GROUP BY sp.plan_name
ORDER BY total_revenue DESC;
GO

-- --------------------------------------------------------------------------
-- 5.5 Device Usage Distribution
-- --------------------------------------------------------------------------
SELECT
    device,
    COUNT(*) AS sessions,
    ROUND(SUM(watch_duration_minutes), 1) AS total_minutes,
    ROUND(AVG(watch_duration_minutes), 1) AS avg_minutes_per_session
FROM dbo.WatchHistory
GROUP BY device
ORDER BY sessions DESC;
GO

-- --------------------------------------------------------------------------
-- 5.6 Top-Rated Movies (min 5 ratings) — CTE + WINDOW FUNCTION
-- --------------------------------------------------------------------------
WITH RatingStats AS (
    SELECT
        r.show_id,
        m.title,
        COUNT(*) AS num_ratings,
        ROUND(AVG(CAST(r.rating AS DECIMAL(4,2))), 2) AS avg_rating
    FROM dbo.Ratings r
    JOIN dbo.Movies m ON r.show_id = m.show_id
    GROUP BY r.show_id, m.title
    HAVING COUNT(*) >= 5
),
Ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (ORDER BY avg_rating DESC, num_ratings DESC) AS rnk
    FROM RatingStats
)
SELECT * FROM Ranked WHERE rnk <= 20
ORDER BY rnk;
GO

-- --------------------------------------------------------------------------
-- 5.7 Monthly Viewing Trend (CTE + date functions)
-- --------------------------------------------------------------------------
WITH MonthlyWatch AS (
    SELECT
        FORMAT(watch_date, 'yyyy-MM') AS watch_month,
        watch_duration_minutes,
        user_id
    FROM dbo.WatchHistory
)
SELECT
    watch_month,
    COUNT(*) AS total_views,
    ROUND(SUM(watch_duration_minutes), 1) AS total_minutes,
    COUNT(DISTINCT user_id) AS unique_viewers
FROM MonthlyWatch
GROUP BY watch_month
ORDER BY watch_month;
GO

-- --------------------------------------------------------------------------
-- 5.8 Churn-Risk Users — CTE + WINDOW FUNCTION
--     Flags users whose most recent watch was >30 / >60 days ago
-- --------------------------------------------------------------------------
WITH LastWatch AS (
    SELECT
        user_id,
        MAX(watch_date) AS last_watch_date,
        COUNT(*) AS total_sessions,
        ROUND(SUM(watch_duration_minutes), 1) AS total_watch_minutes
    FROM dbo.WatchHistory
    GROUP BY user_id
)
SELECT
    lw.user_id,
    u.name,
    lw.last_watch_date,
    DATEDIFF(DAY, lw.last_watch_date, GETDATE()) AS days_since_last_watch,
    lw.total_sessions,
    lw.total_watch_minutes,
    CASE
        WHEN DATEDIFF(DAY, lw.last_watch_date, GETDATE()) > 60 THEN 'High'
        WHEN DATEDIFF(DAY, lw.last_watch_date, GETDATE()) > 30 THEN 'Medium'
        ELSE 'Low'
    END AS churn_risk
FROM LastWatch lw
JOIN dbo.Users u ON lw.user_id = u.user_id
ORDER BY days_since_last_watch DESC;
GO

-- --------------------------------------------------------------------------
-- 5.9 Recommendation-Ready Dataset — Top 3 Genres per User (WINDOW FUNCTION)
-- --------------------------------------------------------------------------
WITH GenreCounts AS (
    SELECT
        w.user_id,
        m.primary_genre,
        COUNT(*) AS genre_view_count
    FROM dbo.WatchHistory w
    JOIN dbo.Movies m ON w.show_id = m.show_id
    GROUP BY w.user_id, m.primary_genre
),
RankedGenres AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY genre_view_count DESC) AS genre_rank
    FROM GenreCounts
)
SELECT user_id, primary_genre, genre_view_count, genre_rank
FROM RankedGenres
WHERE genre_rank <= 3
ORDER BY user_id, genre_rank;
GO

-- --------------------------------------------------------------------------
-- 5.10 VIEWS — reusable KPI views for Power BI / Tableau to connect to
-- --------------------------------------------------------------------------
IF OBJECT_ID('dbo.vw_TopWatchedTitles', 'V') IS NOT NULL DROP VIEW dbo.vw_TopWatchedTitles;
GO
CREATE VIEW dbo.vw_TopWatchedTitles AS
SELECT
    m.show_id, m.title, m.type, m.primary_genre,
    COUNT(*) AS total_views,
    SUM(w.watch_duration_minutes) AS total_watch_minutes
FROM dbo.WatchHistory w
JOIN dbo.Movies m ON w.show_id = m.show_id
GROUP BY m.show_id, m.title, m.type, m.primary_genre;
GO

IF OBJECT_ID('dbo.vw_RevenueByPlan', 'V') IS NOT NULL DROP VIEW dbo.vw_RevenueByPlan;
GO
CREATE VIEW dbo.vw_RevenueByPlan AS
SELECT
    sp.plan_name,
    COUNT(*) AS subscription_count,
    SUM(s.monthly_revenue) AS total_revenue
FROM dbo.Subscriptions s
JOIN dbo.SubscriptionPlans sp ON s.plan_id = sp.plan_id
GROUP BY sp.plan_name;
GO

IF OBJECT_ID('dbo.vw_UserEngagement', 'V') IS NOT NULL DROP VIEW dbo.vw_UserEngagement;
GO
CREATE VIEW dbo.vw_UserEngagement AS
SELECT
    w.user_id,
    COUNT(*) AS total_sessions,
    SUM(w.watch_duration_minutes) AS total_watch_minutes,
    MAX(w.watch_date) AS last_watch_date
FROM dbo.WatchHistory w
GROUP BY w.user_id;
GO

-- Example: query the views directly
-- SELECT TOP 10 * FROM dbo.vw_TopWatchedTitles ORDER BY total_views DESC;
-- SELECT * FROM dbo.vw_RevenueByPlan ORDER BY total_revenue DESC;
