# Microsoft Fabric Spark notebook - Production synthetic data generator
# Generates 1M+ rows with proper statistical distribution from source data

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql.window import Window
from functools import reduce  # For combining DataFrames

# ============================================
# CONFIGURATION - YOU CONTROL THIS
# ============================================
TARGET_ROW_COUNT = 1000  # Set your desired number of rows here
BATCH_SIZE = 10  # Process in batches for better memory management
RANDOM_SEED = 42  # For reproducibility

print(f"🚀 Starting synthetic data generation for {TARGET_ROW_COUNT:,} records")
print(f"📊 Batch size: {BATCH_SIZE:,}, Random seed: {RANDOM_SEED}")

# ============================================
# STEP 1: Analyze source data distribution
# ============================================
print("\n📊 Step 1: Analyzing source data distribution...")

source_df = spark.sql("SELECT * FROM Lake24.dbo.person")
source_count = source_df.count()
print(f"   Source table has {source_count:,} records")

# Get max person_id for sequential generation
max_id_result = source_df.agg(max("person_id")).collect()[0][0]
max_id = max_id_result if max_id_result is not None else 0
print(f"   Current max person_id: {max_id}")

# ============================================
# STEP 2: Calculate statistical distributions
# ============================================
print("\n📈 Step 2: Calculating statistical distributions...")

# Function to get weighted distribution
def get_weighted_distribution(df, column):
    """Returns DataFrame with column values and their weights"""
    if df.filter(col(column).isNotNull()).count() == 0:
        return None
    
    total = df.count()
    dist_df = df.filter(col(column).isNotNull()) \
        .groupBy(column) \
        .count() \
        .withColumn("weight", col("count") / total) \
        .select(column, "weight")
    
    return dist_df

# Get distributions for each column
gender_dist = get_weighted_distribution(source_df, "gender_concept_id")
race_dist = get_weighted_distribution(source_df, "race_concept_id")
ethnicity_dist = get_weighted_distribution(source_df, "ethnicity_concept_id")

# Get year statistics
year_stats = source_df.agg(
    min("year_of_birth").alias("min_year"),
    max("year_of_birth").alias("max_year"),
    percentile_approx("year_of_birth", 0.5).alias("median_year"),
    avg("year_of_birth").alias("mean_year"),
    stddev("year_of_birth").alias("stddev_year")
).collect()[0]

print(f"   Year range: {year_stats['min_year']} - {year_stats['max_year']}")
print(f"   Mean year: {year_stats['mean_year']:.0f}, StdDev: {year_stats['stddev_year']:.1f}")

# ============================================
# STEP 3: Prepare distribution data for broadcast
# ============================================
print("\n🔄 Step 3: Preparing distribution data...")

# Convert to pandas for easier handling in UDFs
if gender_dist:
    gender_pdf = gender_dist.toPandas()
    gender_values = gender_pdf['gender_concept_id'].tolist()
    gender_weights = gender_pdf['weight'].tolist()
else:
    gender_values = [8507, 8532]  # Default if no data
    gender_weights = [0.5, 0.5]

if race_dist:
    race_pdf = race_dist.toPandas()
    race_values = race_pdf['race_concept_id'].tolist()
    race_weights = race_pdf['weight'].tolist()
else:
    race_values = [8527, 8515, 8552]
    race_weights = [0.7, 0.2, 0.1]

if ethnicity_dist:
    ethnicity_pdf = ethnicity_dist.toPandas()
    ethnicity_values = ethnicity_pdf['ethnicity_concept_id'].tolist()
    ethnicity_weights = ethnicity_pdf['weight'].tolist()
else:
    ethnicity_values = [0, 1]
    ethnicity_weights = [0.8, 0.2]

# Broadcast to all nodes
gender_values_bc = spark.sparkContext.broadcast(gender_values)
gender_weights_bc = spark.sparkContext.broadcast(gender_weights)
race_values_bc = spark.sparkContext.broadcast(race_values)
race_weights_bc = spark.sparkContext.broadcast(race_weights)
ethnicity_values_bc = spark.sparkContext.broadcast(ethnicity_values)
ethnicity_weights_bc = spark.sparkContext.broadcast(ethnicity_weights)

# ============================================
# STEP 4: Define UDFs for weighted random selection
# ============================================
print("\n⚙️ Step 4: Defining selection functions...")

# Weighted random selector UDFs
@udf(returnType=IntegerType())
def weighted_gender():
    if not gender_values_bc.value:
        return None
    return int(np.random.choice(
        gender_values_bc.value, 
        p=gender_weights_bc.value
    ))

@udf(returnType=IntegerType())
def weighted_race():
    if not race_values_bc.value:
        return None
    return int(np.random.choice(
        race_values_bc.value, 
        p=race_weights_bc.value
    ))

@udf(returnType=IntegerType())
def weighted_ethnicity():
    if not ethnicity_values_bc.value:
        return None
    return int(np.random.choice(
        ethnicity_values_bc.value, 
        p=ethnicity_weights_bc.value
    ))

# ============================================
# STEP 5: Generate synthetic data in batches
# ============================================
print(f"\n🛠️ Step 5: Generating {TARGET_ROW_COUNT:,} synthetic records...")

# Set random seed for reproducibility
spark.sparkContext.setLogLevel("WARN")
np.random.seed(RANDOM_SEED)

# Generate in batches to avoid memory issues
num_batches = (TARGET_ROW_COUNT + BATCH_SIZE - 1) // BATCH_SIZE
all_batches = []

for batch_num in range(num_batches):
    batch_start = batch_num * BATCH_SIZE
    import builtins
    batch_end = builtins.min((batch_num + 1) * BATCH_SIZE, TARGET_ROW_COUNT)
    batch_rows = batch_end - batch_start
    
    print(f"   Processing batch {batch_num + 1}/{num_batches} ({batch_rows:,} rows)...")
    
    # Create batch DataFrame
    batch_df = spark.range(batch_rows) \
        .withColumn("batch_offset", lit(batch_start)) \
        .withColumn("person_id", col("id") + max_id + batch_start + 1) \
        .withColumn("year_of_birth", 
                    (rand() * (year_stats['max_year'] - year_stats['min_year'] + 1) + year_stats['min_year'])
                    .cast("int")) \
        .withColumn("month_of_birth", 
                    (rand() * 12 + 1).cast("int")) \
        .withColumn("day_of_birth",
                    when(col("month_of_birth").isin(4, 6, 9, 11), (rand() * 30 + 1).cast("int"))
                    .when(col("month_of_birth") == 2, (rand() * 28 + 1).cast("int"))
                    .otherwise((rand() * 31 + 1).cast("int"))) \
        .withColumn("birth_datetime",
                    to_timestamp(
                        concat_ws("-",
                            col("year_of_birth").cast("string"),
                            lpad(col("month_of_birth").cast("string"), 2, "0"),
                            lpad(col("day_of_birth").cast("string"), 2, "0")
                        ), "yyyy-MM-dd"
                    )) \
        .withColumn("gender_concept_id", weighted_gender()) \
        .withColumn("race_concept_id", weighted_race()) \
        .withColumn("ethnicity_concept_id", weighted_ethnicity()) \
        .withColumn("location_id", lit(None).cast("int")) \
        .withColumn("provider_id", lit(None).cast("int")) \
        .withColumn("care_site_id", lit(None).cast("int")) \
        .withColumn("person_source_value", 
                    concat(lit("SYNTH_"), lpad(col("person_id").cast("string"), 10, "0"))) \
        .withColumn("gender_source_value", col("gender_concept_id").cast("string")) \
        .withColumn("gender_source_concept_id", col("gender_concept_id")) \
        .withColumn("race_source_value", col("race_concept_id").cast("string")) \
        .withColumn("race_source_concept_id", col("race_concept_id")) \
        .withColumn("ethnicity_source_value", col("ethnicity_concept_id").cast("string")) \
        .withColumn("ethnicity_source_concept_id", col("ethnicity_concept_id")) \
        .drop("id", "batch_offset")
    
    all_batches.append(batch_df)

# Combine all batches - FIXED: Use reduce with union
print(f"\n🔄 Combining {len(all_batches)} batches...")
if len(all_batches) > 1:
    # Use reduce to union all DataFrames
    synthetic_df = reduce(lambda df1, df2: df1.union(df2), all_batches)
else:
    synthetic_df = all_batches[0]

# ============================================
# STEP 6: Validate generated data
# ============================================
print("\n✅ Step 6: Validating generated data...")

generated_count = synthetic_df.count()
print(f"   Generated {generated_count:,} records")

# Show sample
print("\n📋 Sample of generated data (first 10 rows):")
synthetic_df.show(10, truncate=False)

# Validate distributions
print("\n📊 Gender distribution comparison:")
synthetic_df.groupBy("gender_concept_id") \
    .agg(count("*").alias("count")) \
    .withColumn("percentage", col("count") / generated_count * 100) \
    .show()

print("\n📊 Year of birth statistics:")
synthetic_df.select(
    min("year_of_birth").alias("min_year"),
    max("year_of_birth").alias("max_year"),
    avg("year_of_birth").alias("avg_year")
).show()

# ============================================
# STEP 7: Write to target table
# ============================================
print("\n💾 Step 7: Writing to Lake24.dbo.person...")

# Write with optimization
synthetic_df.write \
    .mode("append") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .option("delta.autoOptimize", "true") \
    .saveAsTable("Lake24.dbo.person")

# ============================================
# STEP 8: Verify final count
# ============================================
print("\n🔍 Step 8: Verifying final table count...")

final_count = spark.sql("SELECT COUNT(*) FROM Lake24.dbo.person").collect()[0][0]
print(f"   Final table count: {final_count:,} records")
print(f"   New records added: {generated_count:,}")

print("\n" + "="*50)
print(f"🎉 SUCCESS: Generated and appended {generated_count:,} synthetic records")
print("="*50)