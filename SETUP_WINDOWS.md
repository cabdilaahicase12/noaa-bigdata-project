# Setup Guide — Windows 10/11 with 16 GB RAM

This guide assumes you have transferred this project folder to a Windows
laptop with **16 GB RAM and 80+ GB free disk**.

Plan ~3–4 hours total. Most of that is waiting (downloads, ETL).

---

## Step 1. Install Docker Desktop (one-time, ~20–30 min)

1. Go to <https://www.docker.com/products/docker-desktop/>, download Docker Desktop for Windows, and run the installer.
2. When asked, **enable "Use WSL 2 instead of Hyper-V"** (recommended).
3. Reboot Windows when prompted.
4. Launch Docker Desktop. The whale icon in the tray should turn green.
5. In Docker Desktop → **Settings → Resources → Advanced**, set:
   - Memory: **10 GB** (leaves 6 GB for Windows)
   - CPUs: **at least 4**
   - Disk image size: **80 GB**
   Click **Apply & restart**.

Sanity check in PowerShell:
```powershell
docker --version
docker-compose --version
```

## Step 2. Open the project folder

In PowerShell:
```powershell
cd path\to\noaa-spark-hadoop-rstudio
```
Replace `path\to\` with where you unzipped this folder (for example `cd C:\Users\you\Desktop\noaa-spark-hadoop-rstudio`).

## Step 3. Download the NOAA data (~10–15 min)

```powershell
.\scripts\download_noaa.ps1
```

This downloads `2020.csv.gz` through `2024.csv.gz` (~150 MB each) into `data\noaa\`. If PowerShell blocks the script with a security warning, run once:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```
and then re-run the download.

When it finishes, `data\noaa\` should contain 5 `.csv.gz` files totalling ~750 MB.

## Step 4. Start the cluster + run ETL (~30–45 min, mostly Spark crunching)

```powershell
.\scripts\run_pipeline.ps1
```

This script will:

1. `docker-compose up -d` — pulls images the first time (~5 min download), then starts all 9 containers.
2. Waits 90 s for HDFS/Hive/Spark to come online.
3. Uploads the NOAA CSVs from `data/noaa/` into HDFS at `/user/spark/noaa_raw/`.
4. Runs `noaa_etl.py` on the Spark cluster — this parses ~150 million rows, writes Parquet partitioned by year, and registers the Hive table.

While it runs, you can watch progress in your browser:

- **Spark Master UI** — <http://localhost:8080> (you should see 2 workers)
- **HDFS NameNode UI** — <http://localhost:9870> (you should see 2 live datanodes)

The Spark stage that writes Parquet is the slow part. Wait for the final line `>>> ETL complete.`

## Step 5. Run the R analysis (~5–10 min)

1. Open <http://localhost:8787>.
2. Log in with **user `rstudio`, password `test`**.
3. In the Files pane, navigate into `R/`.
4. Open `analysis.R`.
5. Click **Source** (or press Ctrl+Shift+S) to run the whole script.

It will install sparklyr/dplyr/ggplot2 the first time (one-time, a few minutes), connect to the cluster, query the Hive table, and save three PNGs into `/home/rstudio/graphics/`.

Back on Windows, those same PNGs appear in `report\graphics\`.

## Step 6. Fill in the report

Open `report\report.md`. Three plot placeholders point at the PNGs you just made. Skim the text, adjust any numbers if they differ from your actual run, and you're done.

To convert it to a Word file (for submission), paste the markdown into Word — Word renders headings, tables, and image references directly. Or use Pandoc:
```powershell
pandoc report\report.md -o report\report.docx
```

---

## Troubleshooting

**"Cannot connect to the Docker daemon"** — Docker Desktop isn't running. Launch it from the Start menu and wait for the whale icon to turn green.

**Containers keep restarting / running out of memory** — close other apps, then in Docker Desktop → Settings → Resources, raise Memory to 11–12 GB. If still tight, comment out `datanode2` and `spark-worker-2` in `docker-compose.yml` (you'll still have 1 NameNode + 1 DataNode + 1 Master + 1 Worker — a valid, smaller distributed cluster).

**Spark ETL hangs at "0 stages"** — the Spark workers haven't registered with the master yet. Wait another minute and check <http://localhost:8080>. If workers don't appear, restart the Spark containers:
```powershell
docker-compose restart spark-master spark-worker-1 spark-worker-2
```
then re-run the ETL:
```powershell
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/noaa_etl.py
```

**RStudio can't connect to Spark** — make sure the Spark master container is healthy (`docker ps` should show it `Up`). The `analysis.R` script connects to `spark://spark-master:7077` from inside Docker's network; this works because RStudio is also a container on the same network.

**"Address already in use"** — another program is using one of these ports: 9870, 8080, 8787, 9000, 7077. Either close that program or change the port mapping in `docker-compose.yml`.

**To shut down everything cleanly when finished:**
```powershell
docker-compose down       # stops and removes the containers
# add -v to ALSO delete the HDFS data (frees ~10 GB)
docker-compose down -v
```
