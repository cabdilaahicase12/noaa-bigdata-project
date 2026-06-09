# NOAA GHCN-Daily Big-Data Analysis

> Graduate course project — **Büyük Veri İşleme Teknikleri ve Uygulamaları**
> Prepared by **Abdullahi Mohamed Abdullah** (Student No: 2528190501)
> Instructor: Assist. Prof. Dr. Ömer Faruk Acar

## Overview

A distributed big-data pipeline that ingests **150+ million real NOAA weather
observations** (GHCN-Daily, 2020–2024), stores them in HDFS as partitioned
Parquet, exposes them through Hive, and analyses them in RStudio with
sparklyr + ggplot2.

## Architecture (9 Docker containers)

| Container | Role |
|---|---|
| `namenode` | HDFS master |
| `datanode1`, `datanode2` | HDFS workers |
| `postgres` | Hive metastore database |
| `hive-metastore` | Schema catalog |
| `spark-master` | Spark cluster manager |
| `spark-worker-1`, `spark-worker-2` | Spark executors |
| `rstudio` | Analysis & visualisation |

## Technologies

- Apache Spark 3.2 (1 master, 2 workers)
- Hadoop HDFS 3.2 (1 NameNode, 2 DataNodes)
- Hive Metastore (PostgreSQL backing store)
- Apache Parquet (Snappy compression)
- RStudio (sparklyr + ggplot2)
- Docker Compose

## Quick start (Windows)

```powershell
.\scripts\download_noaa.ps1     # 1. pull 5 years of NOAA data (~750 MB)
.\scripts\run_pipeline.ps1      # 2. start cluster + ingest + ETL
# 3. open http://localhost:8787  user: rstudio  password: test
#    then Run All in R/analysis.R
```

See `SETUP_WINDOWS.md` for the full step-by-step guide.

## Analyses produced

1. **Annual temperature trend** with linear-regression overlay
2. **Seasonal cycle** — average TMAX per month
3. **Extreme-heat days per year** (TMAX > 35 °C)

All three plots are saved to `report/graphics/` and embedded in `report/report.md`.
