# NOAA GHCN-Daily Big-Data Analysis

> A distributed big-data pipeline that ingests, processes, and analyses **90 million real weather observations** from NOAA's Global Historical Climatology Network on a 9-container **Apache Spark + Hadoop + Hive + RStudio** cluster orchestrated by **Docker Compose**.

![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.2.1-E25A1C?style=flat-square&logo=apachespark)
![Hadoop HDFS](https://img.shields.io/badge/Hadoop_HDFS-3.2-FE6F00?style=flat-square&logo=apachehadoop)
![Hive](https://img.shields.io/badge/Apache_Hive-2.3-FDEE21?style=flat-square&logo=apachehive)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-11-336791?style=flat-square&logo=postgresql)
![Parquet](https://img.shields.io/badge/Parquet-Snappy-50ABF1?style=flat-square)
![R](https://img.shields.io/badge/R-sparklyr-276DC3?style=flat-square&logo=r)
![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?style=flat-square&logo=docker)

---

## 📋 Project Information

| | |
|---|---|
| **Course** | Büyük Veri İşleme Teknikleri ve Uygulamaları |
| **Student** | Abdullahi Mohamed Abdullah |
| **Student No** | 2528190501 |
| **Course Instructor** | Assist. Prof. Dr. Ömer Faruk Acar |
| **Dataset** | NOAA GHCN-Daily — 2020 to 2024 |

---

## 🎯 At a Glance

| Metric | Value |
|---|---:|
| Total rows processed (after QC filtering) | **90,744,056** |
| Years analysed | **5** (2020–2024) |
| Raw download size (compressed) | **~756 MB** |
| Cluster containers | **9** |
| Spark workers | **2** (4 cores, 3 GB total) |
| HDFS replication factor | **2** |
| ETL runtime | **~30 minutes** |
| Output format | **Snappy-compressed Parquet** partitioned by year |

---

## 🏗️ System Architecture

The system runs as 9 Docker containers in 3 logical tiers:

```
 ┌─────────────────────────┐    ┌─────────────────────────┐    ┌──────────────────────────┐
 │     HDFS STORAGE TIER   │    │   SPARK COMPUTE TIER    │    │  CATALOG & ANALYSIS TIER │
 │                         │    │                         │    │                          │
 │   ┌─────────────────┐   │    │   ┌─────────────────┐   │    │  ┌────────────────────┐  │
 │   │    namenode     │   │    │   │  spark-master   │   │    │  │   hive-metastore   │  │
 │   │   HDFS Master   │   │    │   │ Cluster manager │   │    │  │   Thrift  :9083    │  │
 │   └─────────────────┘   │    │   └─────────────────┘   │    │  └────────────────────┘  │
 │   ┌─────────────────┐   │ ─► │   ┌─────────────────┐   │ ─► │  ┌────────────────────┐  │
 │   │    datanode1    │   │    │   │ spark-worker-1  │   │    │  │     postgres       │  │
 │   │  Block storage  │   │    │   │ 2 cores · 1.5GB │   │    │  │   Metastore DB     │  │
 │   └─────────────────┘   │    │   └─────────────────┘   │    │  └────────────────────┘  │
 │   ┌─────────────────┐   │    │   ┌─────────────────┐   │    │  ┌────────────────────┐  │
 │   │    datanode2    │   │    │   │ spark-worker-2  │   │    │  │   rstudio :8787    │  │
 │   │  Block storage  │   │    │   │ 2 cores · 1.5GB │   │    │  │ sparklyr + ggplot2 │  │
 │   └─────────────────┘   │    │   └─────────────────┘   │    │  └────────────────────┘  │
 └─────────────────────────┘    └─────────────────────────┘    └──────────────────────────┘
```

**Data flow:** HDFS → Spark Workers → sparklyr in RStudio. Schema is registered in the Hive Metastore (backed by PostgreSQL).

---

## ⚙️ Technology Stack

| Component | Version | Role |
|---|---|---|
| Apache Spark | 3.2.1 | Distributed in-memory compute |
| Hadoop HDFS | 3.2 | Distributed file system |
| Apache Hive | 2.3 | Schema catalog (Metastore) |
| PostgreSQL | 11 | Backing store for Hive Metastore |
| Apache Parquet | + Snappy | Columnar storage format |
| sparklyr | latest | R interface to Spark |
| ggplot2 | latest | Plot rendering |
| RStudio Server | 4.3.2 | Browser-based R IDE |
| Docker Compose | v2 | 9-container orchestration |

---

## 📊 Results

### 1. Annual Temperature Trend

Average **TMAX** across all stations and years:

| Year | Avg TMAX (°C) |
|:---:|:---:|
| 2020 | 16.9 |
| 2021 | 16.8 |
| 2022 | **17.9** |
| 2023 | 17.1 |
| 2024 | 17.7 |

The 5-year linear regression confirms an overall upward trend.

### 2. Monthly Seasonality

Average TMAX aggregated by calendar month shows a clear Northern-Hemisphere-dominated seasonal cycle (trough around January/February, peak around July/August), reflecting the geographic distribution of GHCN reporting stations.

### 3. Extreme-Heat Days per Year

Total station-days where **TMAX > 35 °C**:

| Year | Station-days |
|:---:|---:|
| 2020 | 164,972 |
| 2021 | 158,156 |
| 2022 | 139,889 |
| 2023 | 189,002 |
| 2024 | **196,501** |

> 📈 **+40 % increase** from 2022 to 2024 — consistent with widely reported global heat records.

Plots and CSV results are in [`results/`](./results/).

---

## 🚀 Quick Start (Windows + Docker Desktop)

### Prerequisites

- Windows 10/11 with **16 GB RAM** and **80 GB free disk**
- **Docker Desktop 4.35.x** with WSL 2 backend (Docker Engine Memory ≥ 10 GB in Settings → Resources)
- BIOS Intel Virtualization Technology (VT-x) enabled

### Run the pipeline

```powershell
# 1. Allow scripts for this session
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# 2. Download 5 years of NOAA data (~750 MB) into data/noaa/
.\scripts\download_noaa.ps1

# 3. Start the 9-container cluster
docker-compose up -d

# 4. Wait ~90 s for HDFS / Hive / Spark to initialise, then upload data to HDFS
docker exec namenode hdfs dfs -mkdir -p /user/spark/noaa_raw
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 777 /user
docker exec namenode bash -c "hdfs dfs -put -f /data/noaa/*.csv.gz /user/spark/noaa_raw/"

# 5. Run the Spark ETL (parses ~150 M rows → Parquet on HDFS, ~30 min)
docker exec spark-master /spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.sql.catalogImplementation=hive `
    /jobs/noaa_etl.py

# 6. Open RStudio in your browser → user: rstudio, password: test
#    Then Source R/analysis.R
start http://localhost:8787
```

### Web UIs (while cluster runs)

| URL | Purpose |
|---|---|
| http://localhost:9870 | HDFS NameNode UI — see live DataNodes & block reports |
| http://localhost:8080 | Spark Master UI — workers, running apps, history |
| http://localhost:8787 | RStudio Server — login `rstudio` / `test` |

### Shutdown

```powershell
docker-compose down       # stops containers, keeps data volumes (safe)
# docker-compose down -v  # ⚠️ ALSO deletes HDFS data — only use if completely done
```

---

## 📁 Repository Structure

```
noaa-spark-hadoop-rstudio/
├── docker-compose.yml          # 9-container cluster definition
├── config/
│   ├── hadoop.env              # HDFS / YARN / Hive env vars
│   ├── hive-site.xml           # Hive Metastore configuration
│   └── spark-defaults.conf     # Spark tuning for 16 GB host
├── jobs/
│   └── noaa_etl.py             # PySpark ETL (CSV → Hive-managed Parquet)
├── R/
│   └── analysis.R              # sparklyr + ggplot2 analyses
├── scripts/
│   ├── download_noaa.ps1       # Pull 5 years of NOAA data from AWS S3 mirror
│   └── run_pipeline.ps1        # End-to-end Windows orchestrator
├── results/                    # Output plots & CSVs from the R analysis
│   ├── 01_annual_trend.png
│   ├── 02_seasonality.png
│   ├── 03_extreme_heat.png
│   ├── annual_trend.csv
│   ├── seasonality.csv
│   └── extreme_heat.csv
├── NOAA_BigData_Project.pptx   # Presentation slide deck
├── SETUP_WINDOWS.md            # Full Windows setup walkthrough
└── README.md                   # You are here
```

---

## 🔬 ETL Pipeline Detail

The ETL job ([`jobs/noaa_etl.py`](./jobs/noaa_etl.py)) performs five stages:

| # | Stage | What it does |
|---|---|---|
| 1 | **Ingest** | Reads 5 yearly `*.csv.gz` files from `/user/spark/noaa_raw/` on HDFS |
| 2 | **Parse** | Applies explicit 8-column schema (station, date, element, value, flags, obs_time) |
| 3 | **Clean** | Drops QC-flagged rows; keeps only TMAX / TMIN / PRCP elements |
| 4 | **Enrich** | Derives `year`, `month`, `day`, and a typed `date` field from the YYYYMMDD string |
| 5 | **Persist** | Writes Snappy-compressed Parquet **partitioned by year** to HDFS with replication = 2 |

Output location: `hdfs://namenode:9000/user/hive/warehouse/weather.db/noaa_observations/year={2020,2021,2022,2023,2024}/`

---

## 🧠 Defense Q&A (Short Form)

<details>
<summary><b>Is the data synthetic?</b></summary>

No. 90.7 million real measurements from NOAA's GHCN-Daily archive, downloaded from the official **NOAA-AWS Open Data mirror** at `noaa-ghcn-pds.s3.amazonaws.com`. Same source used in published climate research.
</details>

<details>
<summary><b>Why this dataset?</b></summary>

Large-scale (10⁸ rows in this 5-year slice, 10⁹+ in the full archive), free, well-documented, scientifically meaningful. A real big-data problem — not synthetic or toy data.
</details>

<details>
<summary><b>Is the system actually distributed?</b></summary>

Yes — verifiable via the running web UIs:
- 2 HDFS DataNodes with replication factor 2 (every block on both nodes)
- 2 Spark Workers executing in parallel (4 cores, 3 GB total)
- Hive Metastore as a separate Thrift service
- PostgreSQL as a separate metastore database

9 containers, demonstrable live at `localhost:9870` and `localhost:8080`.
</details>

<details>
<summary><b>Why Parquet over CSV?</b></summary>

Columnar storage + Snappy compression means smaller files and fast scans of column subsets. Year-based partitioning enables **partition pruning** — queries on a single year skip entire files instead of scanning the whole dataset.
</details>

<details>
<summary><b>Why Spark instead of Pandas?</b></summary>

Pandas would have to load all ~6 GB uncompressed into a single machine's RAM. Spark distributes the work across workers — each handles a partition, and only the small aggregated result returns to the driver. The same code would scale to terabytes on a multi-node cluster.
</details>

<details>
<summary><b>What analyses did you run?</b></summary>

Three quantitative analyses on the Parquet warehouse, all from RStudio via sparklyr:
1. Annual TMAX trend with linear regression
2. Monthly seasonality (TMAX by calendar month, all years)
3. Extreme-heat station-days per year (TMAX > 35 °C)
</details>

---

## 🔮 Future Work

- Extend ingestion to the full GHCN-Daily archive (100+ years instead of 5)
- Add a station-metadata join (lat/long/elevation) to enable geographic and latitudinal analyses
- Deploy on a multi-node Kubernetes cluster with YARN instead of single-host Docker Compose
- Apply Spark MLlib for forecasting and regression on TMAX
- Build a real-time Kafka ingestion path for the daily GHCN updates

---

## 📚 References

1. NOAA National Centers for Environmental Information — *Global Historical Climatology Network – Daily (GHCN-Daily)*. <https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily>
2. NOAA on AWS Open Data Registry. <https://registry.opendata.aws/noaa-ghcn/>
3. Apache Spark 3.2.1 documentation. <https://spark.apache.org/docs/3.2.1/>
4. Apache Hadoop 3.2 documentation. <https://hadoop.apache.org/docs/r3.2.1/>
5. `sparklyr` — R interface for Apache Spark. <https://spark.rstudio.com>
6. Apache Parquet file format. <https://parquet.apache.org>

---

## 📄 License & Acknowledgements

This is an academic project submitted for the *Büyük Veri İşleme Teknikleri ve Uygulamaları* graduate course.

Data © **NOAA** — public domain.
Container images by **Big Data Europe** (`bde2020/*`) and **The Rocker Project** (`rocker/rstudio`).
