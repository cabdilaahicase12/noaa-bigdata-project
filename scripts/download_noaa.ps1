# ============================================================================
# download_noaa.ps1
# Downloads 5 years of NOAA GHCN-Daily "by_year" CSV files into data/noaa/.
# Each year is ~150 MB compressed, ~30-40 million rows.  5 years -> ~150M rows,
# well above the 10M-row project requirement.
#
# Adjust $years if you want fewer/more years.
# ============================================================================

$years = 2020..2024
$dest  = ".\data\noaa"

# NOAA's primary path (current).  Fallback URL kept in case NCEI moves it.
$base1 = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/by_year"
$base2 = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year"

New-Item -ItemType Directory -Force -Path $dest | Out-Null
Write-Host "Downloading NOAA GHCN-Daily into $dest" -ForegroundColor Cyan

foreach ($y in $years) {
    $file = "$y.csv.gz"
    $out  = Join-Path $dest $file
    if (Test-Path $out) {
        Write-Host "  already have $file" -ForegroundColor DarkGray
        continue
    }

    $ok = $false
    foreach ($base in @($base1, $base2)) {
        $url = "$base/$file"
        try {
            Write-Host "  downloading $url" -ForegroundColor Yellow
            Invoke-WebRequest -Uri $url -OutFile $out -UseBasicParsing -TimeoutSec 600
            $ok = $true
            break
        } catch {
            Write-Host "    failed: $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    if (-not $ok) {
        Write-Host "  ERROR: could not download $file from either NOAA mirror." -ForegroundColor Red
        Write-Host "  Check your internet connection and try again, or download $file" -ForegroundColor Red
        Write-Host "  manually from $base1 and place it in $dest" -ForegroundColor Red
    }
}

Write-Host "`nFinal contents of ${dest}:" -ForegroundColor Cyan
Get-ChildItem $dest | Format-Table Name, @{Name="SizeMB";Expression={[math]::Round($_.Length/1MB,1)}}
