library(googlesheets4)
library(tidygeocoder)
library(tidyverse)

data <- read_sheet("https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1103432292#gid=1103432292",
                   sheet = "domains_with_coverage_progress")

# extract sample of 25% of observations (random)
set.seed(123)  # for reproducibility
data_ <- data |> 
  sample_frac(0.20)

# note which observations in data are inside the sample using a join
data <- data |> 
  left_join(data_ |> select(domain) |> mutate(sample = TRUE), by = "domain") |> 
  mutate(sample = ifelse(is.na(sample), FALSE, sample))

# geocode the domains
data_geocoded <- data |> 
  geocode(primary_location, method = "osm", lat = "lat", long = "lon", full_results = TRUE)

googlesheets4::sheet_write(data_geocoded, 
                           ss = "https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1103432292#gid=1103432292",
                           sheet = "domains_with_coverage_progress_geocoded")
