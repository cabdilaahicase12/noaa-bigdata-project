# ============================================================================
# run_pipeline.ps1
# End-to-end runner: brings up the cluster, ingests NOAA data into HDFS,
# runs the Spark ETL.  After this finishes, open RStudio and run R/analysis.R.
#
# Prereqs:
#   1. Docker Desktop is running.
#   2. .\scripts\download_noaa.ps1 has finished (CSVs present in data\noaa\).
# ============================================================================

$ErrorActionPreference = "Stop"

Write-Host "==> 1/5  Starting the cluster (this can take a few minutes the first time)" -ForegroundColor Cyan
docker-compose up -d

Write-Host "==> 2/5  Waiting 90 s for HDFS / Hive / Spark to come up..." -ForegroundColor Cyan
Start-Sleep -Seconds 90

Write-Host "==> 3/5  Creating HDFS directory and uploading NOAA CSVs" -ForegroundColor Cyan
docker exec namenode hdfs dfs -mkdir -p /user/spark/noaa_raw
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 777 /user

# the raw CSVs are visible inside namenode (and spark-master) at /data/noaa
docker exec namenode bash -c "hdfs dfs -put -f /data/noaa/*.csv.gz /user/spark/noaa_raw/"
docker exec namenode hdfs dfs -ls /user/spark/noaa_raw

Write-Host "==> 4/5  Running the Spark ETL (this is the long step — 15-30 min)" -ForegroundColor Cyan
docker exec spark-master /spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.sql.catalogImplementation=hive `
    /jobs/noaa_etl.py

Write-Host "==> 5/5  ETL complete.  Web UIs:" -ForegroundColor Green
Write-Host "  Spark Master : http://localhost:8080" -ForegroundColor Green
Write-Host "  HDFS         : http://localhost:9870" -ForegroundColor Green
Write-Host "  RStudio      : http://localhost:8787  (user: rstudio  password: test)" -ForegroundColor Green
Write-Host "`nNext:  open RStudio in your browser, log in, and Run All in R/analysis.R" -ForegroundColor Yellow
