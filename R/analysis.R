# NOAA GHCN-Daily Analysis - reads Parquet directly from HDFS

required <- c("sparklyr", "dplyr", "ggplot2")
new <- setdiff(required, installed.packages()[, "Package"])
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")

library(sparklyr)
library(dplyr)
library(ggplot2)

sc <- spark_connect(
  master  = "spark://spark-master:7077",
  spark_home = "/opt/spark",
  config  = list(
    spark.executor.memory = "1g",
    spark.driver.memory   = "600m"
  )
)
cat(">>> connected to Spark\n")

noaa <- spark_read_parquet(
  sc, "noaa",
  path = "hdfs://namenode:9000/user/hive/warehouse/weather.db/noaa_observations"
)

total <- noaa %>% summarise(n = n()) %>% collect() %>% pull(n)
cat(">>> total rows:", format(total, big.mark = ","), "\n")

# Analysis 1: annual trend
annual <- noaa %>%
  filter(element == "TMAX") %>%
  group_by(year) %>%
  summarise(avg_temp = mean(value / 10, na.rm = TRUE)) %>%
  arrange(year) %>%
  collect()
print(annual)

p1 <- ggplot(annual, aes(x = year, y = avg_temp)) +
  geom_line(color = "steelblue", size = 1.2) +
  geom_point(color = "steelblue", size = 3) +
  geom_smooth(method = "lm", se = TRUE, color = "firebrick") +
  labs(title = "Global Average Maximum Temperature",
       subtitle = "NOAA GHCN-Daily 2020-2024",
       x = "Year", y = "Average TMAX (deg C)") +
  theme_minimal(base_size = 13)
ggsave("/home/rstudio/graphics/01_annual_trend.png", p1, width = 9, height = 5, dpi = 120)
cat(">>> saved 01_annual_trend.png\n")

# Analysis 2: seasonality
seasonal <- noaa %>%
  filter(element == "TMAX") %>%
  group_by(month) %>%
  summarise(avg_temp = mean(value / 10, na.rm = TRUE)) %>%
  arrange(month) %>%
  collect()
months <- c("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")
seasonal$month_name <- factor(months[seasonal$month], levels = months)

p2 <- ggplot(seasonal, aes(x = month_name, y = avg_temp)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = round(avg_temp, 1)), vjust = -0.4, size = 3.5) +
  labs(title = "Average Maximum Temperature by Month",
       subtitle = "NOAA GHCN-Daily 2020-2024",
       x = "Month", y = "Average TMAX (deg C)") +
  theme_minimal(base_size = 13)
ggsave("/home/rstudio/graphics/02_seasonality.png", p2, width = 9, height = 5, dpi = 120)
cat(">>> saved 02_seasonality.png\n")

# Analysis 3: extreme heat
extremes <- noaa %>%
  filter(element == "TMAX", value > 350) %>%
  group_by(year) %>%
  summarise(extreme_days = n()) %>%
  arrange(year) %>%
  collect()
print(extremes)

p3 <- ggplot(extremes, aes(x = factor(year), y = extreme_days)) +
  geom_col(fill = "darkorange") +
  geom_text(aes(label = format(extreme_days, big.mark = ",")), vjust = -0.4, size = 3.5) +
  labs(title = "Extreme-Heat Days per Year (TMAX > 35 deg C)",
       subtitle = "NOAA GHCN-Daily 2020-2024",
       x = "Year", y = "Station-days") +
  theme_minimal(base_size = 13)
ggsave("/home/rstudio/graphics/03_extreme_heat.png", p3, width = 9, height = 5, dpi = 120)
cat(">>> saved 03_extreme_heat.png\n")

write.csv(annual, "/home/rstudio/graphics/annual_trend.csv", row.names = FALSE)
write.csv(seasonal, "/home/rstudio/graphics/seasonality.csv", row.names = FALSE)
write.csv(extremes, "/home/rstudio/graphics/extreme_heat.csv", row.names = FALSE)

spark_disconnect(sc)
cat(">>> done\n")
