from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import split, explode, col, count, avg, round, sum, first, desc, row_number
from pyspark.sql.functions import from_unixtime, to_timestamp
import json

# 1. Spark session oluştur
spark = SparkSession.builder \
    .appName("Stocks table joiner") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# AWS credentials
spark.conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

BUCKET_RAW = "nyse-market-pipeline-raw-data"
BUCKET_ANALYTICS = "nyse-market-pipeline-analytics-data"

# 2. AWS'den oku
print("Reading data from AWS...")
fundamentals = spark.read.csv(
    f"s3a://{BUCKET_RAW}/fundamentals.csv",
    header=True,
    inferSchema=True
)

prices = spark.read.csv(
    f"s3a://{BUCKET_RAW}/prices.csv",
    header=True,
    inferSchema=True
)
securities = spark.read.csv(
    f"s3a://{BUCKET_RAW}/securities.csv",
    header=True,
    inferSchema=True
)

print(f"All tables have been read")

# 3. Transformations

# Önce DataFrame'i geçici tablo olarak kaydet
prices.createOrReplaceTempView("prices")
fundamentals.createOrReplaceTempView("fundamentals")
securities.createOrReplaceTempView("securities")

# SQL sorgumuz
prices_yearly = spark.sql("""
    WITH prices_with_window AS (
    SELECT 
        symbol,
        YEAR(date) as year,
        high,
        low,
        FIRST_VALUE(close) OVER (PARTITION BY symbol, YEAR(date) ORDER BY date) as first_close,
        LAST_VALUE(close) OVER (PARTITION BY symbol, YEAR(date) ORDER BY date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_close
    FROM prices
    )
    SELECT
        symbol,
        year,
        MAX(first_close) as first_close,
        MAX(last_close) as last_close,
        AVG(high - low) as avg_daily_range
    FROM prices_with_window
    GROUP BY symbol, year
""")

# 4. Join işlemleri
print("Joining order_items with payments...")

prices_yearly.createOrReplaceTempView("prices_yearly")

stocks_enriched = spark.sql("""
    SELECT 
        p.symbol,
        p.year,
        p.first_close,
        p.last_close,
        p.avg_daily_range,
        s.`GICS Sector` as sector,
        s.`GICS Sub Industry` as sub_industry,
        s.Security as company_name,
        f.`Period Ending` as period_ending,
        f.`Total Revenue` as total_revenue,
        f.`Net Income` as net_income,
        f.`Gross Margin` as gross_margin,
        f.`Operating Margin` as operating_margin,
        f.`Earnings Per Share` as eps,
        f.`Long-Term Debt` as long_term_debt,
        f.`Total Equity` as total_equity
    FROM prices_yearly p
    INNER JOIN fundamentals f ON p.symbol = f.`Ticker Symbol` 
    AND YEAR(f.`Period Ending`) = p.year
    INNER JOIN securities s ON p.symbol = s.`Ticker Symbol`
""")

# 4.5 Drop Unnecassary Columns


# 5. Parquet olarak S3'e yaz
print("Writing parquet to S3...")
stocks_enriched.write.mode("overwrite").parquet(
    f"s3a://{BUCKET_ANALYTICS}/spark/stocks_enriched"
)
print("Parquet written successfully!")

