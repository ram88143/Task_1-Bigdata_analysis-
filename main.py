# -------------------------------------------
# 🏠 Big Data Analysis: Global Housing Market
# 📊 Using PySpark
# -------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc
import sys

# 1️⃣ Initialize Spark Session
spark = SparkSession.builder \
    .appName("Global Housing Market Analysis") \
    .getOrCreate()

# 2️⃣ Load Dataset
file_path = "global_housing_market_extended.csv"

try:
    df = spark.read.option("header", True).csv(file_path, inferSchema=True)
    print("\n✅ Dataset loaded successfully!\n")
except Exception as e:
    print(f"❌ Error loading file: {e}")
    sys.exit(1)

# 3️⃣ Display Schema and Sample Records
print("🔍 Dataset Schema:")
df.printSchema()

print("\n🔎 First 5 Records:")
df.show(5, truncate=False)

# 4️⃣ Data Cleaning
# Drop rows missing critical information
columns_to_check = ["Country", "House Price Index", "GDP Growth (%)"]
df_clean = df.dropna(subset=columns_to_check)

print(f"\n🧹 Cleaned data: {df_clean.count()} rows remaining after dropping nulls.")

# 5️⃣ Insights & Analysis

# a. 🌍 Average House Price Index by Country
print("\n🏠 Top 10 Countries by Avg. House Price Index:")
df_clean.groupBy("Country") \
    .agg(avg("House Price Index").alias("Avg_House_Price_Index")) \
    .orderBy(desc("Avg_House_Price_Index")) \
    .show(10, truncate=False)

# b. 📈 Average GDP Growth Rate by Country
print("\n📊 Top 10 Countries by Avg. GDP Growth:")
df_clean.groupBy("Country") \
    .agg(avg("GDP Growth (%)").alias("Avg_GDP_Growth")) \
    .orderBy(desc("Avg_GDP_Growth")) \
    .show(10, truncate=False)

# c. 📌 Top 5 Countries with the Most Records
print("\n🧾 Top 5 Countries by Number of Records:")
df_clean.groupBy("Country") \
    .agg(count("*").alias("Record_Count")) \
    .orderBy(desc("Record_Count")) \
    .show(5, truncate=False)

# 6️⃣ Shut Down Spark
spark.stop()
print("\n✅ Spark session stopped. Analysis complete.")
