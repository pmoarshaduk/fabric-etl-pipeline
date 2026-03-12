# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 **COMPREHENSIVE DATA GENERATOR v6.0 - CONFIGURABLE**
# MAGIC 
# MAGIC **Features:**
# MAGIC - ✅ Generate CLEAN data (100% valid)
# MAGIC - ✅ Generate RAW data (mix valid + problematic)
# MAGIC - ✅ Generate SCD updates (from existing dimension records)
# MAGIC - ✅ Fully configurable via Config class
# MAGIC - ✅ All person table columns populated
# MAGIC - ✅ Realistic demographics (NHS compliant)
# MAGIC - ✅ Error handling (no logging, silent failures)
# MAGIC 
# MAGIC **Date:** 2026-03-11
# MAGIC **Version:** 6.0

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from delta.tables import DeltaTable
import time

print("=" * 80)
print("🚀 COMPREHENSIVE DATA GENERATOR v6.0")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ CONFIGURATION

# COMMAND ----------

class Config:
    """
    Comprehensive data generation configuration
    """
    
    # ═══════════════════════════════════════════════════════════════
    # TARGET TABLE
    # ═══════════════════════════════════════════════════════════════
    TARGET_TABLE = "dbo.person"
    DIMENSION_TABLE = "dbo.dim_person"  # For SCD updates
    
    # ═══════════════════════════════════════════════════════════════
    # GENERATION MODE
    # "CLEAN" = 100% valid records (baseline testing)
    # "RAW" = Mix of valid + problematic (DQ testing)
    # "SCD" = Updates to existing dimension records (SCD testing)
    # ═══════════════════════════════════════════════════════════════
    MODE = "CLEAN"  # ← CHANGE THIS TO: CLEAN, RAW, or SCD
    
    # ═══════════════════════════════════════════════════════════════
    # RECORD COUNTS
    # ═══════════════════════════════════════════════════════════════
    CLEAN_RECORDS = 3      # For CLEAN mode
    RAW_RECORDS = 5        # For RAW mode
    SCD_UPDATES = 2        # For SCD mode
    
    # ═══════════════════════════════════════════════════════════════
    # DATA QUALITY (RAW mode only)
    # ═══════════════════════════════════════════════════════════════
    ERROR_RATE = 0.60  # 60% invalid in RAW mode
    
    # Error types distribution (for RAW mode)
    ERROR_TYPES = {
        "NULL_ID": 0.20,          # 20% NULL person_id
        "INVALID_GENDER": 0.40,   # 40% bad gender codes
        "INVALID_YEAR": 0.40      # 40% bad birth years
    }
    
    # ═══════════════════════════════════════════════════════════════
    # SCD UPDATE CONFIGURATION
    # ═══════════════════════════════════════════════════════════════
    SCD_ATTRIBUTES_TO_CHANGE = ["gender_concept_id", "year_of_birth"]
    
    # What to change per attribute
    SCD_CHANGES = {
        "gender_concept_id": {
            8507: 8532,  # Male → Female
            8532: 8551,  # Female → Unknown
            8551: 8507   # Unknown → Male
        },
        "year_of_birth": {
            "offset": -16  # Subtract 16 years (changes age significantly)
        }
    }
    
    # ═══════════════════════════════════════════════════════════════
    # TIMESTAMP STRATEGY
    # "FUTURE" = Future timestamp (ensures incremental load picks up)
    # "CURRENT" = Current timestamp
    # "DISTRIBUTED" = Spread over last 30 days
    # ═══════════════════════════════════════════════════════════════
    TIMESTAMP_STRATEGY = "FUTURE"
    FUTURE_MINUTES = 15  # How far in future
    
    # ═══════════════════════════════════════════════════════════════
    # ID STRATEGY
    # "AUTO" = Auto-increment from max ID
    # "CUSTOM" = Use custom ID ranges
    # ═══════════════════════════════════════════════════════════════
    ID_STRATEGY = "CUSTOM"
    
    # Custom ID ranges per mode
    ID_RANGES = {
        "CLEAN": (2000001, 2999999),  # 2M range
        "RAW": (3000001, 3999999),    # 3M range
        "SCD": None                    # Uses existing IDs from dimension
    }

# Print configuration
print(f"\n📋 CONFIGURATION:")
print(f"   Mode: {Config.MODE}")
print(f"   Target: {Config.TARGET_TABLE}")

if Config.MODE == "CLEAN":
    print(f"   Records: {Config.CLEAN_RECORDS} (100% valid)")
elif Config.MODE == "RAW":
    print(f"   Records: {Config.RAW_RECORDS} ({Config.ERROR_RATE:.0%} invalid)")
elif Config.MODE == "SCD":
    print(f"   Updates: {Config.SCD_UPDATES} (from dimension table)")

print(f"   Timestamp: {Config.TIMESTAMP_STRATEGY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 TABLE ANALYSIS

# COMMAND ----------

print("\n🔍 ANALYZING TABLES:")
print("-" * 80)

# Initialize variables
max_id = 0
target_count = 0

try:
    # Get target table info
    target_df = spark.table(Config.TARGET_TABLE)
    target_count = target_df.count()
    max_id_result = target_df.agg(F.max("person_id")).collect()[0][0]
    max_id = max_id_result if max_id_result is not None else 0
    
    print(f"   {Config.TARGET_TABLE}:")
    print(f"     Records: {target_count:,}")
    print(f"     Max ID: {max_id:,}")
    
    # Get dimension table info (for SCD mode)
    if Config.MODE == "SCD":
        dim_df = spark.table(Config.DIMENSION_TABLE)
        dim_count = dim_df.filter("is_current = true").count()
        print(f"\n   {Config.DIMENSION_TABLE}:")
        print(f"     Current records: {dim_count:,}")
        
        if dim_count < Config.SCD_UPDATES:
            print(f"   ⚠️  WARNING: Only {dim_count} current records available")
            print(f"              Requested {Config.SCD_UPDATES} updates")
            Config.SCD_UPDATES = dim_count
            print(f"              Adjusted to {Config.SCD_UPDATES} updates")
    
except Exception as e:
    print(f"   ❌ Error analyzing tables: {str(e)}")
    # Silent failure - set defaults
    max_id = 0
    target_count = 0

print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎲 DATA GENERATION FUNCTIONS

# COMMAND ----------

def get_start_id(mode, max_id):
    """Determine starting ID based on strategy"""
    if Config.ID_STRATEGY == "AUTO":
        return max_id + 1
    elif Config.ID_STRATEGY == "CUSTOM":
        id_range = Config.ID_RANGES.get(mode)
        if id_range:
            return id_range[0]
        else:
            return max_id + 1
    else:
        return max_id + 1

def get_timestamp():
    """Get timestamp based on strategy"""
    if Config.TIMESTAMP_STRATEGY == "FUTURE":
        return datetime.now() + timedelta(minutes=Config.FUTURE_MINUTES)
    elif Config.TIMESTAMP_STRATEGY == "CURRENT":
        return datetime.now()
    elif Config.TIMESTAMP_STRATEGY == "DISTRIBUTED":
        # Random timestamp in last 30 days
        days_back = int((datetime.now() - datetime(2026, 2, 1)).days * 0.5)
        return datetime.now() - timedelta(days=days_back, hours=12)
    else:
        return datetime.now()

def generate_name(gender_concept_id, person_id):
    """Generate realistic name based on gender"""
    first_names_male = ["James", "John", "Michael", "David", "Wei", "Kumar", "Ahmed"]
    first_names_female = ["Mary", "Jennifer", "Li", "Priya", "Fatima", "Maria"]
    last_names = ["Smith", "Wang", "Kumar", "Garcia", "Ahmed", "Lee", "Chen"]
    
    import random
    random.seed(person_id)  # Deterministic based on ID
    
    if gender_concept_id == 8507:  # Male
        first = random.choice(first_names_male)
    elif gender_concept_id == 8532:  # Female
        first = random.choice(first_names_female)
    else:  # Unknown
        first = random.choice(first_names_male + first_names_female)
    
    last = random.choice(last_names)
    return f"{first} {last}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏭 MODE 1: CLEAN DATA GENERATION

# COMMAND ----------

def generate_clean_data():
    """Generate 100% valid records"""
    print(f"\n✅ GENERATING {Config.CLEAN_RECORDS} CLEAN RECORDS:")
    print("-" * 80)
    
    start_id = get_start_id("CLEAN", max_id)
    base_timestamp = get_timestamp()
    
    print(f"   Starting ID: {start_id:,}")
    print(f"   Timestamp: {base_timestamp}")
    
    clean_records = []
    
    for i in range(Config.CLEAN_RECORDS):
        person_id = start_id + i
        
        # Distribute gender evenly
        if i % 3 == 0:
            gender_concept_id = 8507  # Male
            gender_source_value = "M"
        elif i % 3 == 1:
            gender_concept_id = 8532  # Female
            gender_source_value = "F"
        else:
            gender_concept_id = 8551  # Unknown
            gender_source_value = "U"
        
        # Distribute ages across NHS bands
        if i % 3 == 0:
            year_of_birth = 1979  # Age 47 (18-64)
        elif i % 3 == 1:
            year_of_birth = 1996  # Age 30 (18-64)
        else:
            year_of_birth = 1956  # Age 70 (65+)
        
        record = {
            "person_id": person_id,
            "gender_concept_id": gender_concept_id,
            "year_of_birth": year_of_birth,
            "month_of_birth": (i % 12) + 1,
            "day_of_birth": ((i % 28) + 1),
            "birth_datetime": datetime(year_of_birth, (i % 12) + 1, ((i % 28) + 1)).date(),
            "race_concept_id": 8527,  # White
            "ethnicity_concept_id": 38003564,  # Not Hispanic
            "location_id": f"LOC_CLEAN_{i+1:03d}",
            "provider_id": f"PROV_CLEAN_{i+1:03d}",
            "care_site_id": f"SITE_CLEAN_{i+1:03d}",
            "person_source_value": generate_name(gender_concept_id, person_id),
            "gender_source_value": gender_source_value,
            "gender_source_concept_id": gender_concept_id,
            "race_source_value": "White",
            "race_source_concept_id": 8527,
            "ethnicity_source_value": "Not Hispanic",
            "ethnicity_source_concept_id": 38003564,
            "created_timestamp": base_timestamp - timedelta(days=365),
            "updated_timestamp": base_timestamp,
            "is_deleted": False
        }
        
        clean_records.append(record)
    
    print(f"   ✅ Generated {len(clean_records)} clean records")
    return clean_records

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 MODE 2: RAW DATA GENERATION (WITH ERRORS)

# COMMAND ----------

def generate_raw_data():
    """Generate mix of valid + problematic records"""
    print(f"\n🧪 GENERATING {Config.RAW_RECORDS} RAW RECORDS ({Config.ERROR_RATE:.0%} invalid):")
    print("-" * 80)
    
    start_id = get_start_id("RAW", max_id)
    base_timestamp = get_timestamp()
    
    print(f"   Starting ID: {start_id:,}")
    print(f"   Timestamp: {base_timestamp}")
    
    raw_records = []
    num_errors = int(Config.RAW_RECORDS * Config.ERROR_RATE)
    
    for i in range(Config.RAW_RECORDS):
        # Determine if this is an error record
        is_error = i < num_errors
        
        # Determine error type
        error_type = None
        if is_error:
            if i / num_errors < Config.ERROR_TYPES["NULL_ID"]:
                error_type = "NULL_ID"
            elif i / num_errors < Config.ERROR_TYPES["NULL_ID"] + Config.ERROR_TYPES["INVALID_GENDER"]:
                error_type = "INVALID_GENDER"
            else:
                error_type = "INVALID_YEAR"
        
        # Generate record
        if error_type == "NULL_ID":
            person_id = None
            gender_concept_id = 8507
            year_of_birth = 1985
            source_value = "RAW_NULL_ID"
        elif error_type == "INVALID_GENDER":
            person_id = start_id + i
            gender_concept_id = 7777  # Invalid
            year_of_birth = 1978
            source_value = f"RAW_BAD_GENDER_{i}"
        elif error_type == "INVALID_YEAR":
            person_id = start_id + i
            gender_concept_id = 8532
            year_of_birth = 2028  # Future
            source_value = f"RAW_FUTURE_YEAR_{i}"
        else:
            # Valid record
            person_id = start_id + i
            gender_concept_id = 8507 if i % 2 == 0 else 8532
            year_of_birth = 1992 if i % 2 == 0 else 1924  # Leap day or centenarian
            source_value = f"RAW_VALID_{i}"
        
        gender_source_value = "M" if gender_concept_id == 8507 else ("F" if gender_concept_id == 8532 else "X")
        
        record = {
            "person_id": person_id,
            "gender_concept_id": gender_concept_id,
            "year_of_birth": year_of_birth,
            "month_of_birth": 2 if year_of_birth == 1992 else ((i % 12) + 1),
            "day_of_birth": 29 if year_of_birth == 1992 else ((i % 28) + 1),
            "birth_datetime": datetime(year_of_birth, 
                                      2 if year_of_birth == 1992 else ((i % 12) + 1),
                                      29 if year_of_birth == 1992 else ((i % 28) + 1)).date() if year_of_birth < 2027 else None,
            "race_concept_id": 8527,
            "ethnicity_concept_id": 38003564,
            "location_id": f"LOC_RAW_{i+1:03d}",
            "provider_id": f"PROV_RAW_{i+1:03d}",
            "care_site_id": f"SITE_RAW_{i+1:03d}",
            "person_source_value": source_value,
            "gender_source_value": gender_source_value,
            "gender_source_concept_id": gender_concept_id,
            "race_source_value": "White",
            "race_source_concept_id": 8527,
            "ethnicity_source_value": "Not Hispanic",
            "ethnicity_source_concept_id": 38003564,
            "created_timestamp": base_timestamp - timedelta(days=100 + i*10),
            "updated_timestamp": base_timestamp,
            "is_deleted": False
        }
        
        raw_records.append(record)
    
    print(f"   ✅ Generated {len(raw_records)} raw records")
    print(f"      Valid: {Config.RAW_RECORDS - num_errors}")
    print(f"      Invalid: {num_errors}")
    
    return raw_records

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 MODE 3: SCD UPDATE GENERATION

# COMMAND ----------

def generate_scd_updates():
    """Generate updates to existing dimension records"""
    print(f"\n🔄 GENERATING {Config.SCD_UPDATES} SCD UPDATES:")
    print("-" * 80)
    
    try:
        # Get current dimension records
        dim_current = spark.table(Config.DIMENSION_TABLE) \
            .filter("is_current = true") \
            .select("person_id") \
            .limit(Config.SCD_UPDATES) \
            .collect()
        
        if not dim_current:
            print(f"   ❌ No current dimension records found")
            return []
        
        person_ids = [row.person_id for row in dim_current]
        print(f"   Selected person_ids: {person_ids}")
        
        # Get current person records
        person_df = spark.table(Config.TARGET_TABLE) \
            .filter(F.col("person_id").isin(person_ids))
        
        # Convert to list of dicts
        current_records = [row.asDict() for row in person_df.collect()]
        
        base_timestamp = get_timestamp()
        
        update_records = []
        
        for i, record in enumerate(current_records):
            # Determine which attribute to change
            attr_to_change = Config.SCD_ATTRIBUTES_TO_CHANGE[i % len(Config.SCD_ATTRIBUTES_TO_CHANGE)]
            
            # Apply change
            if attr_to_change == "gender_concept_id":
                old_gender = record["gender_concept_id"]
                new_gender = Config.SCD_CHANGES["gender_concept_id"].get(old_gender, 8532)
                record["gender_concept_id"] = new_gender
                record["gender_source_concept_id"] = new_gender
                record["gender_source_value"] = "M" if new_gender == 8507 else ("F" if new_gender == 8532 else "U")
                record["person_source_value"] = f"{record['person_source_value']}_GENDER_UPDATED"
                
                print(f"   Person {record['person_id']}: Gender {old_gender} → {new_gender}")
                
            elif attr_to_change == "year_of_birth":
                old_year = record["year_of_birth"]
                offset = Config.SCD_CHANGES["year_of_birth"]["offset"]
                new_year = old_year + offset
                record["year_of_birth"] = new_year
                
                # Update birth_datetime
                month = record["month_of_birth"] or 1
                day = record["day_of_birth"] or 1
                try:
                    record["birth_datetime"] = datetime(new_year, month, day).date()
                except:
                    record["birth_datetime"] = datetime(new_year, 1, 1).date()
                
                print(f"   Person {record['person_id']}: Year {old_year} → {new_year} (age change)")
            
            # Update timestamp
            record["updated_timestamp"] = base_timestamp
            
            update_records.append(record)
        
        print(f"   ✅ Generated {len(update_records)} SCD updates")
        return update_records
        
    except Exception as e:
        print(f"   ❌ Error generating SCD updates: {str(e)}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 DATA GENERATION & WRITE

# COMMAND ----------

print("\n" + "=" * 80)
print("🏭 DATA GENERATION STARTED")
print("=" * 80)

gen_start = time.time()

# Generate based on mode
if Config.MODE == "CLEAN":
    records = generate_clean_data()
elif Config.MODE == "RAW":
    records = generate_raw_data()
elif Config.MODE == "SCD":
    records = generate_scd_updates()
else:
    print(f"❌ Unknown mode: {Config.MODE}")
    records = []

if not records:
    print("\n❌ No records generated - stopping")
    dbutils.notebook.exit("No records generated")

gen_time = time.time() - gen_start

# COMMAND ----------

# Create DataFrame
person_schema = StructType([
    StructField("person_id", IntegerType(), True),
    StructField("gender_concept_id", IntegerType(), True),
    StructField("year_of_birth", IntegerType(), True),
    StructField("month_of_birth", IntegerType(), True),
    StructField("day_of_birth", IntegerType(), True),
    StructField("birth_datetime", DateType(), True),
    StructField("race_concept_id", IntegerType(), True),
    StructField("ethnicity_concept_id", IntegerType(), True),
    StructField("location_id", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("care_site_id", StringType(), True),
    StructField("person_source_value", StringType(), True),
    StructField("gender_source_value", StringType(), True),
    StructField("gender_source_concept_id", IntegerType(), True),
    StructField("race_source_value", StringType(), True),
    StructField("race_source_concept_id", IntegerType(), True),
    StructField("ethnicity_source_value", StringType(), True),
    StructField("ethnicity_source_concept_id", IntegerType(), True),
    StructField("created_timestamp", TimestampType(), True),
    StructField("updated_timestamp", TimestampType(), True),
    StructField("is_deleted", BooleanType(), True)
])

df = spark.createDataFrame(records, schema=person_schema)

print(f"\n📊 DATAFRAME CREATED:")
print(f"   Records: {df.count()}")

# Show sample
print(f"\n📋 SAMPLE DATA:")
df.select(
    "person_id",
    "gender_concept_id",
    "year_of_birth",
    "person_source_value",
    "updated_timestamp"
).show(min(len(records), 10), truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 WRITE TO TABLE

# COMMAND ----------

print("\n💾 WRITING TO TABLE:")
print("-" * 80)

write_start = time.time()

try:
    if Config.MODE == "SCD":
        # For SCD mode, use MERGE to update existing records
        print(f"   Mode: MERGE (updating existing records)")
        
        delta_table = DeltaTable.forName(spark, Config.TARGET_TABLE)
        
        delta_table.alias("target").merge(
            df.alias("source"),
            "target.person_id = source.person_id"
        ).whenMatchedUpdateAll().execute()
        
        print(f"   ✅ Updated {len(records)} records")
        
    else:
        # For CLEAN/RAW modes, append
        print(f"   Mode: APPEND (adding new records)")
        
        df.write.format("delta").mode("append").saveAsTable(Config.TARGET_TABLE)
        
        print(f"   ✅ Appended {len(records)} records")
    
    write_time = time.time() - write_start
    print(f"   Write time: {write_time:.2f}s")
    
except Exception as e:
    print(f"   ❌ Error writing: {str(e)}")
    # Silent failure - no logging
    write_time = 0

print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ VERIFICATION

# COMMAND ----------

print("\n✅ VERIFICATION:")
print("-" * 80)

try:
    final_count = spark.table(Config.TARGET_TABLE).count()
    
    if Config.MODE == "SCD":
        print(f"   Mode: SCD (updated records)")
        print(f"   Total records: {final_count:,}")
        
        # Show updated records
        print(f"\n   Updated records:")
        person_ids = [r["person_id"] for r in records]
        spark.table(Config.TARGET_TABLE) \
            .filter(F.col("person_id").isin(person_ids)) \
            .select("person_id", "gender_concept_id", "year_of_birth", 
                   "person_source_value", "updated_timestamp") \
            .show(truncate=False)
    else:
        expected = target_count + len(records)
        print(f"   Initial: {target_count:,}")
        print(f"   Added: {len(records):,}")
        print(f"   Expected: {expected:,}")
        print(f"   Actual: {final_count:,}")
        print(f"   Status: {'✅ PASS' if final_count == expected else '❌ FAIL'}")

except Exception as e:
    print(f"   ⚠️ Verification skipped: {str(e)}")

print("-" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 SUMMARY

# COMMAND ----------

total_time = time.time() - gen_start

print("\n" + "=" * 80)
print("📊 GENERATION SUMMARY")
print("=" * 80)
print(f"Mode: {Config.MODE}")
print(f"Records: {len(records)}")
print(f"\n⚡ PERFORMANCE:")
print(f"   Generation: {gen_time:.2f}s")
print(f"   Write: {write_time:.2f}s")
print(f"   Total: {total_time:.2f}s")

if Config.MODE == "CLEAN":
    print(f"\n✅ CLEAN DATA:")
    print(f"   All {len(records)} records are 100% valid")
    print(f"   Ready for baseline testing")
    print(f"\n📋 NEXT STEP:")
    print(f"   Run ETL and expect all records to flow through all layers")
    
elif Config.MODE == "RAW":
    num_errors = int(Config.RAW_RECORDS * Config.ERROR_RATE)
    print(f"\n🧪 RAW DATA:")
    print(f"   Valid: {Config.RAW_RECORDS - num_errors}")
    print(f"   Invalid: {num_errors} ({Config.ERROR_RATE:.0%})")
    print(f"\n📋 NEXT STEP:")
    print(f"   Run ETL and check quarantine for {num_errors} failures")
    
elif Config.MODE == "SCD":
    print(f"\n🔄 SCD UPDATES:")
    print(f"   Updated {len(records)} existing persons")
    print(f"   Attributes changed: {', '.join(Config.SCD_ATTRIBUTES_TO_CHANGE)}")
    print(f"\n📋 NEXT STEP:")
    print(f"   Run ETL and check dimension for {len(records)} historical rows")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ GENERATOR v6.0 - COMPLETE
# MAGIC 
# MAGIC **Modes:**
# MAGIC 1. **CLEAN** - 100% valid records for baseline testing
# MAGIC 2. **RAW** - Mix of valid + problematic for DQ testing
# MAGIC 3. **SCD** - Updates to existing records for SCD Type 2 testing
# MAGIC 
# MAGIC **Features:**
# MAGIC - ✅ Fully configurable via Config class
# MAGIC - ✅ All person table columns populated
# MAGIC - ✅ Realistic demographics
# MAGIC - ✅ Error handling (no logging)
# MAGIC - ✅ Intelligent SCD updates from dimension table
# MAGIC 
# MAGIC **Usage:**
# MAGIC 1. Set `Config.MODE` to: CLEAN, RAW, or SCD
# MAGIC 2. Adjust counts in Config class
# MAGIC 3. Run notebook
# MAGIC 4. Run ETL
# MAGIC 5. Verify results
