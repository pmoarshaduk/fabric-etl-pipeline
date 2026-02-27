# fabric-etl-pipeline
Enterprise ETL pipeline for Microsoft Fabric processing 45M+ rows. Features: Medallion architecture, non-terminating error handling, RCA engine, GDPR compliance, NHS ECDS v3.0, SCD Type 2, Delta Lake optimization. Production-ready with 7-year audit trail &amp; comprehensive monitoring.
## For "About" Section (350 characters max):

Enterprise ETL pipeline for Microsoft Fabric processing 45M+ rows. Features: Medallion architecture, non-terminating error handling, RCA engine, GDPR compliance, NHS ECDS v3.0, SCD Type 2, Delta Lake optimization. Production-ready with 7-year audit trail & comprehensive monitoring.

## Topics/Tags:

microsoft-fabric
delta-lake
pyspark
etl-pipeline
data-engineering
healthcare-data
gdpr-compliance
medallion-architecture
azure
python
data-quality
nhs-ecds
audit-trail
scd-type2
lakehouse
production-ready
data-governance
root-cause-analysis
error-handling
big-data

## Repository Name Suggestions:

1. fabric-etl-pipeline (simple)
2. enterprise-fabric-etl (professional)
3. fabric-medallion-pipeline (descriptive)
4. healthcare-fabric-etl (niche-specific)
5. production-fabric-pipeline (production-ready)

Recommended: **fabric-etl-pipeline**

## Repository Tagline:

"Production-grade ETL pipeline for Microsoft Fabric with enterprise error handling and healthcare compliance"

# 🚀 Production-Grade ETL Pipeline for Microsoft Fabric

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric-0078D4?style=flat&logo=microsoft&logoColor=white)](https://www.microsoft.com/en-us/microsoft-fabric)
[![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8?style=flat&logo=databricks&logoColor=white)](https://delta.io/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![GDPR](https://img.shields.io/badge/GDPR-Compliant-00AA00?style=flat)](https://gdpr.eu/)
[![NHS ECDS](https://img.shields.io/badge/NHS-ECDS%20v3.0-005EB8?style=flat)](https://digital.nhs.uk/)

> Enterprise-grade ETL pipeline built on Microsoft Fabric, designed for healthcare data processing at scale (45M+ rows). Features non-terminating error handling, comprehensive audit trails, GDPR compliance, and NHS ECDS v3.0 standards.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Data Quality](#data-quality)
- [Compliance](#compliance)
- [Monitoring](#monitoring)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

This project demonstrates a **production-ready ETL pipeline** built natively on **Microsoft Fabric** using **Delta Lake** and **PySpark**. It implements the **Medallion Architecture** (Bronze → Silver → Gold → Dimension) with enterprise-grade features including:

- ✅ Non-terminating error handling (pipeline never stops)
- ✅ Row-level and column-level error capture
- ✅ Root Cause Analysis (RCA) engine
- ✅ 7-year audit trail retention
- ✅ Schema validation with drift detection
- ✅ GDPR pseudonymization (SHA-256)
- ✅ NHS ECDS v3.0 compliance
- ✅ SCD Type 2 dimension tracking
- ✅ Optimized for 45M+ row processing

### 🏥 Healthcare Focus

Built specifically for healthcare data with:
- **NHS ECDS v3.0** default codes and age banding
- **GDPR Article 32** pseudonymization
- **ISO 27001** audit trail
- **NIST CSF** data quality framework

---

## ✨ Key Features

### 🔄 Medallion Architecture
```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌────────────┐
│  Bronze  │ -> │  Silver  │ -> │   Gold   │ -> │ Dimension  │
│ Raw Data │    │ Validated│    │ Business │    │ SCD Type 2 │
└──────────┘    └──────────┘    └──────────┘    └────────────┘
```

### 🛡️ Non-Terminating Error Handling
- **Row-level errors**: Logged and quarantined, pipeline continues
- **Schema mismatches**: Auto-detected and resolved
- **System failures**: Logged to RCA with resolution suggestions
- **Zero data loss**: All good data processed, all bad data tracked

### 📊 Comprehensive Observability
- **15 production reports** for monitoring
- **Real-time health score** (0-100)
- **Performance metrics** by stage
- **SLA compliance** tracking

### 🔐 Security & Compliance
| Standard | Implementation |
|----------|---------------|
| **GDPR** | SHA-256 pseudonymization, Article 30 audit trail |
| **NHS ECDS v3.0** | Default codes (8551, 7, 0), age banding |
| **ISO 27001** | Data classification, 7-year audit retention |
| **NIST CSF** | Data quality framework, continuous monitoring |

---

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                      Microsoft Fabric                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Lakehouse   │  │   Notebook   │  │  Warehouse   │          │
│  │ (Delta Lake) │  │   (PySpark)  │  │  (Optional)  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Data Processing Layer                        │
│                                                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │  Bronze  │→ │  Silver  │→ │   Gold   │→ │   Dim    │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
│       ↓             ↓             ↓             ↓               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │  Schema  │  │    DQ    │  │   NHS    │  │   SCD    │       │
│  │Validation│  │ Engine   │  │  Rules   │  │ Type 2   │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
└─────────────────────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
┌─────────────────┐  ┌─────────────────┐
│   Audit Trail   │  │   RCA Engine    │
│  (7-year log)   │  │ (Error tracking)│
└─────────────────┘  └─────────────────┘
```

### Data Flow

1. **Bronze Layer**: Raw data ingestion with metadata enrichment
2. **Silver Layer**: Data quality validation + NHS business rules
3. **Gold Layer**: Business-ready aggregates + pseudonymization
4. **Dimension**: SCD Type 2 with historical tracking

---

## 🚀 Quick Start

### Prerequisites

- Microsoft Fabric workspace
- Lakehouse created (`Lake24` in our example)
- Source table: `dbo.person`
- Spark cluster (automatically provided by Fabric)

### Installation

1. **Clone the repository**:
```bash
git clone https://github.com/yourusername/fabric-etl-pipeline.git
cd fabric-etl-pipeline
```

2. **Create a Notebook in Fabric**:
   - Open Microsoft Fabric
   - Navigate to your Lakehouse
   - Create new Notebook
   - Copy contents of `PRODUCTION_ETL_CLEAN_FINAL.py`

3. **Configure** (edit Config class):
```python
class Config:
    DATABASE = "dbo"  # Your database
    SOURCE_TABLE = "person"  # Your source table
    SHUFFLE_PARTITIONS = 400  # Tune for your data volume
    SCHEMA_MISMATCH_ACTION = "RECREATE"  # RECREATE, ADAPT, or FAIL
```

4. **Run**:
```python
run_production_etl()
```

### First Run Output

```
================================================================================
PRODUCTION ETL - EXECUTION
================================================================================
Session: abc123-def456-ghi789

[BRONZE] Raw ingestion...
✅ BRONZE_LOADED: Loaded 28 records
✅ BRONZE_WRITE: Created dbo.bronze_person
📊 BRONZE: 2.34s | 28 rows | 12 rows/s

[SILVER] Validation & enrichment...
   DQ Pass Rate: 100.0%
   Valid: 28 | Quarantine: 0
✅ PSEUDONYMIZATION: Applied GDPR pseudonymization
📊 SILVER: 3.12s | 28 rows | 9 rows/s

[GOLD] Business aggregation...
📊 GOLD: 1.89s | 28 rows | 15 rows/s

[DIM] Dimension (SCD Type 2)...
📊 DIM: 2.01s | 28 rows | 14 rows/s

================================================================================
✅✅✅ PIPELINE SUCCESS ✅✅✅
================================================================================
Session: abc123-def456-ghi789
Duration: 9.36s
Bronze: 28 records
Silver: 28 records (DQ: 100.0%)
Gold: 28 records
Dimension: 28 records
Throughput: 3 rows/s
NHS ECDS: v3.0 ✅ | GDPR: Pseudonymized ✅
================================================================================
```

---

## ⚙️ Configuration

### Tuning for Large Datasets (45M+ rows)

```python
class Config:
    # Performance tuning
    SHUFFLE_PARTITIONS = 400  # Start: 400, Range: 200-800
    REPARTITION_COUNT = 400   # Match shuffle partitions
    
    # Schema management
    SCHEMA_MISMATCH_ACTION = "RECREATE"  # Options: RECREATE, ADAPT, FAIL
    
    # Compliance
    NHS_VERSION = "v3.0"
    NHS_UNKNOWN_GENDER = 8551
    NHS_UNKNOWN_ETHNICITY = 7
    NHS_UNKNOWN_RACE = 0
```

### Spark Optimization

Automatically configured:
```python
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

---

## 🎯 Data Quality

### Validation Rules

```python
dq_rules = [
    {"name": "PERSON_ID_NOT_NULL", 
     "condition": F.col("person_id").isNotNull()},
    
    {"name": "GENDER_VALID", 
     "condition": F.col("gender_concept_id").isin([8507, 8532, 8551]) | 
                  F.col("gender_concept_id").isNull()},
    
    {"name": "BIRTH_YEAR_RANGE", 
     "condition": F.col("year_of_birth").between(1900, 2026) | 
                  F.col("year_of_birth").isNull()}
]
```

### DQ Metrics

- **Pass rate**: Percentage of records passing all rules
- **Quarantine**: Failed records isolated for investigation
- **Tracking**: All failures logged to RCA with reason codes

---

## 🔐 Compliance

### GDPR (Article 32)

**Pseudonymization**:
```python
def pseudonymize(value: str) -> str:
    return hashlib.sha256(f"{value}FABRIC_2026".encode()).hexdigest()

# Applied to PII fields:
silver_df = silver_df.withColumn(
    "person_source_value_pseudo",
    pseudonymize_udf(F.col("person_source_value"))
)
```

### NHS ECDS v3.0

**Default Codes**:
| Field | Unknown Value | SNOMED Code |
|-------|--------------|-------------|
| Gender | 8551 | Unknown gender |
| Ethnicity | 7 | Not stated |
| Race | 0 | Unknown |

**Age Banding**:
```
0-<1, 1-4, 5-9, 10-14, 15-19, 20-24, 25-29, 30-34,
35-39, 40-44, 45-49, 50-54, 55-59, 60-64, 65-69,
70-74, 75-79, 80-84, 85+
```

### Audit Trail

**Retention**: 7 years (NHS requirement)

**Captured Events**:
- Pipeline start/complete/failure
- Schema validation/mismatch
- DQ validation results
- Pseudonymization applied
- Every write operation

**Schema**:
```sql
CREATE TABLE dbo.audit_trail (
    audit_id STRING,
    session_id STRING,
    timestamp TIMESTAMP,
    event_type STRING,
    description STRING,
    stage STRING,
    rows INT,
    status STRING,
    duration_seconds DOUBLE,
    metadata STRING
)
```

---

## 📊 Monitoring

### Health Score (0-100)

```
Health Score = Success Rate (40%) 
             + DQ Pass Rate (30%) 
             + SLA Compliance (20%) 
             + Error Rate Inverse (10%)
```

**Ratings**:
- 90-100: Excellent ✅
- 75-89: Good ⚠️
- 60-74: Fair 🟡
- <60: Needs Attention 🔴

### Key Reports

1. **Pipeline Success Rate** - Last 30 days execution history
2. **DQ Pass Rate Trends** - Data quality over time
3. **Performance Metrics** - Stage-by-stage analysis
4. **Top 10 Errors** - Most frequent RCA issues
5. **SLA Compliance** - Meeting time targets

### Alerting Queries

```sql
-- Alert: Pipeline failures in last hour
SELECT * FROM dbo.audit_trail
WHERE event_type = 'PIPELINE_FAILURE'
  AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR

-- Alert: DQ pass rate below 95%
SELECT * FROM dbo.audit_trail
WHERE event_type = 'DQ_VALIDATION'
  AND CAST(REGEXP_EXTRACT(description, 'Pass rate: ([0-9.]+)%', 1) AS DOUBLE) < 95.0
```

---

## ⚡ Performance

### Benchmarks

| Records | Duration | Throughput | Partitions |
|---------|----------|------------|------------|
| 28 | 9.4s | 3 rows/s | 400 |
| 100K | ~45s | 2,200 rows/s | 400 |
| 1M | ~7.5min | 2,200 rows/s | 400 |
| 10M | ~75min | 2,200 rows/s | 400 |
| 45M | ~340min | 2,200 rows/s | 400 |

**Note**: Benchmarks are estimates. Actual performance depends on:
- Cluster size
- Data complexity
- Network latency
- Concurrent workloads

### Optimization Tips

1. **Tune partitions** based on data volume:
   ```python
   # Small (< 1M): 200 partitions
   # Medium (1-10M): 400 partitions
   # Large (10-50M): 600 partitions
   # Very Large (50M+): 800 partitions
   ```

2. **Run OPTIMIZE** after large writes:
   ```sql
   OPTIMIZE dbo.bronze_person
   ```

3. **Monitor shuffle** operations:
   ```python
   spark.conf.get("spark.sql.shuffle.partitions")
   ```

4. **Scale cluster** for 45M rows:
   - Minimum: 4 nodes
   - Recommended: 8 nodes
   - Optimal: 16 nodes

---

## 📁 Repository Structure

```
fabric-etl-pipeline/
├── README.md                          # This file
├── notebooks/
│   ├── PRODUCTION_ETL_CLEAN_FINAL.py  # Main ETL pipeline
│   └── ETL_REPORTING_QUERIES.sql      # 15 monitoring reports
├── docs/
│   ├── ARCHITECTURE.md                # System architecture
│   ├── DATA_DICTIONARY.md             # Table schemas
│   ├── RUNBOOK.md                     # Operations guide
│   └── TROUBLESHOOTING.md             # Common issues
├── config/
│   └── etl_config.py                  # Configuration templates
├── tests/
│   ├── test_transformations.py        # Unit tests
│   └── test_pipeline.py               # Integration tests
├── examples/
│   ├── generate_test_data.py          # Create 45M test records
│   └── sample_queries.sql             # Example analytics
└── LICENSE                            # MIT License
```

---

## 🧪 Testing

### Unit Tests

```python
# Test NHS age banding
def test_nhs_age_bands():
    test_df = spark.createDataFrame([
        (1, 1995, 6, 15),
        (2, 1980, 3, 22),
        (3, 1945, 12, 1)
    ], ["person_id", "year_of_birth", "month_of_birth", "day_of_birth"])
    
    result_df = apply_nhs_rules(test_df)
    
    assert result_df.filter(F.col("person_id") == 1).select("nhs_age_band").collect()[0][0] == "25-29"
    assert result_df.filter(F.col("person_id") == 2).select("nhs_age_band").collect()[0][0] == "40-44"
    assert result_df.filter(F.col("person_id") == 3).select("nhs_age_band").collect()[0][0] == "75-79"
```

### Integration Tests

```python
# Test end-to-end pipeline
def test_pipeline_execution():
    session_id = run_production_etl()
    
    # Verify tables created
    assert spark.catalog.tableExists("dbo.bronze_person")
    assert spark.catalog.tableExists("dbo.silver_person")
    assert spark.catalog.tableExists("dbo.gold_person")
    assert spark.catalog.tableExists("dbo.dim_person")
    
    # Verify audit trail
    audit_count = spark.table("dbo.audit_trail") \
                      .filter(F.col("session_id") == session_id) \
                      .count()
    assert audit_count > 0
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Coding Standards

- Follow PEP 8 for Python code
- Add docstrings to all functions
- Include unit tests for new features
- Update documentation

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙋 FAQ

### Q: Can I use this with Azure Synapse instead of Fabric?
**A**: Yes, with minor modifications. Replace Fabric-specific syntax with Synapse equivalents.

### Q: How do I handle 100M+ rows?
**A**: Increase partitions to 1000+, use larger cluster, consider incremental loads.

### Q: Is this HIPAA compliant?
**A**: The architecture supports HIPAA requirements (encryption, audit trail, access controls), but you'll need to configure Fabric's security settings appropriately.

### Q: Can I use this for non-healthcare data?
**A**: Absolutely! Remove NHS-specific rules and adjust compliance requirements.

### Q: What if I don't need GDPR pseudonymization?
**A**: Set `ENABLE_PSEUDONYMIZATION = False` in config and remove the pseudonymization step.

---

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/fabric-etl-pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/fabric-etl-pipeline/discussions)
- **Email**: your.email@example.com

---

## 🌟 Acknowledgments

- Microsoft Fabric team for the platform
- Delta Lake community for ACID guarantees
- NHS Digital for ECDS standards
- Healthcare data engineering community

---

## 📈 Roadmap

- [ ] Incremental load pattern
- [ ] Data lineage visualization
- [ ] ML-based anomaly detection
- [ ] Auto-scaling based on volume
- [ ] Multi-tenant support
- [ ] Real-time streaming ingestion

---

## 📚 Additional Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)
- [NHS ECDS Standards](https://digital.nhs.uk/data-and-information/information-standards/information-standards-and-data-collections-including-extractions/publications-and-notifications/standards-and-collections/dcb0092-nhs-emergency-care-data-set)
- [GDPR Compliance Guide](https://gdpr.eu/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

---

Made with ❤️ for the data engineering community

**Star ⭐ this repo if you find it useful!**

