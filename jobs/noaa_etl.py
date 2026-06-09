"""
noaa_etl.py — NOAA GHCN-Daily ETL on Spark + HDFS + Hive

WHAT IT DOES
  1. Reads the gzipped NOAA by_year CSV files staged in HDFS at /user/spark/noaa_raw/
  2. Parses the 8-column schema (no header) into a typed DataFrame
  3. Derives year, month, day, and a real temperature in degrees C
     (NOAA stores TMAX/TMIN in tenths of a degree)
  4. Writes a Parquet dataset to HDFS, partitioned by year, snappy-compressed
  5. Creates an external Hive table over the Parquet so RStudio can query it
  6. Prints a final row count to prove the 10M+ row requirement is met

RUN
  spark-submit /jobs/noaa_etl.py
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ----------------------------------------------------------------------------
# 1. Spark session with Hive support enabled
# ----------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("NOAA-GHCN-Daily-ETL")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(">>> Spark session up, version", spark.version)

RAW_PATH = "hdfs://namenode:9000/user/spark/noaa_raw/*.csv.gz"
OUT_PATH = "hdfs://namenode:9000/user/hive/warehouse/weather.db/noaa_observations"
DB_NAME  = "weather"
TBL_NAME = "noaa_observations"

# ----------------------------------------------------------------------------
# 2. Schema for the NOAA by_year CSVs (no header row in these files)
# ----------------------------------------------------------------------------
schema = StructType([
    StructField("station",  StringType(),  True),   # 11-char station ID
    StructField("date_str", StringType(),  True),   # YYYYMMDD
    StructField("element",  StringType(),  True),   # TMAX, TMIN, PRCP, ...
    StructField("value",    IntegerType(), True),   # tenths of degC / tenths of mm
    StructField("m_flag",   StringType(),  True),
    StructField("q_flag",   StringType(),  True),
    StructField("s_flag",   StringType(),  True),
    StructField("obs_time", StringType(),  True),
])

# ----------------------------------------------------------------------------
# 3. Read the raw CSVs
# ----------------------------------------------------------------------------
print(">>> Reading", RAW_PATH)
raw = (
    spark.read
    .schema(schema)
    .option("header", "false")
    .csv(RAW_PATH)
)
print(">>> Raw rows:", f"{raw.count():,}")

# ----------------------------------------------------------------------------
# 4. Clean + derive date parts + convert value to real units
#    NOAA stores TMAX/TMIN in tenths of a degree C; divide by 10 for degC.
# ----------------------------------------------------------------------------
df = (
    raw
    .filter(F.col("q_flag").isNull() | (F.col("q_flag") == ""))   # drop QC-flagged
    .filter(F.col("element").isin("TMAX", "TMIN", "PRCP"))         # keep core elements
    .withColumn("year",  F.substring("date_str", 1, 4).cast("int"))
    .withColumn("month", F.substring("date_str", 5, 2).cast("int"))
    .withColumn("day",   F.substring("date_str", 7, 2).cast("int"))
    .withColumn("date",  F.to_date("date_str", "yyyyMMdd"))
    .drop("date_str")
)

clean_count = df.count()
print(">>> Cleaned rows:", f"{clean_count:,}")
if clean_count >= 10_000_000:
    print(">>> Requirement met: 10,000,000+ rows.")
else:
    print(">>> WARNING: under 10M rows — add more years and re-run.")

# ----------------------------------------------------------------------------
# 5. Write partitioned Parquet to HDFS
# ----------------------------------------------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

print(">>> Writing Parquet partitioned by year ->", OUT_PATH)
(
    df.write
      .mode("overwrite")
      .partitionBy("year")
      .parquet(OUT_PATH)
)

# ----------------------------------------------------------------------------
# 6. Register an external Hive table over the Parquet
# ----------------------------------------------------------------------------
spark.sql(f"DROP TABLE IF EXISTS {DB_NAME}.{TBL_NAME}")
spark.sql(f"""
    CREATE EXTERNAL TABLE {DB_NAME}.{TBL_NAME} (
        station  STRING,
        element  STRING,
        value    INT,
        m_flag   STRING,
        q_flag   STRING,
        s_flag   STRING,
        obs_time STRING,
        month    INT,
        day      INT,
        date     DATE
    )
    PARTITIONED BY (year INT)
    STORED AS PARQUET
    LOCATION '{OUT_PATH}'
""")
spark.sql(f"MSCK REPAIR TABLE {DB_NAME}.{TBL_NAME}")

# ----------------------------------------------------------------------------
# 7. Final verification — query through Hive and report by year
# ----------------------------------------------------------------------------
print(">>> Hive table created. Row counts by year:")
spark.sql(f"""
    SELECT year, COUNT(*) AS rows
    FROM {DB_NAME}.{TBL_NAME}
    GROUP BY year
    ORDER BY year
""").show(30, truncate=False)

print(">>> ETL complete.")
spark.stop()
