# ==============================================================================
# SETUP: LIBRARIES & DATA PREPARATION
# ==============================================================================
library(tidyverse)
library(sf)
library(spdep)
library(concaveman)
library(ineq)
library(geosphere)
library(DescTools)

outlets <- readRDS("data/analysis_small.rds")

to_remove <- outlets |>
  group_by(domain) |>
  summarise(total_locs = sum(n), .groups = "drop") |>
  filter(total_locs < 10) |>
  pull(domain)

outlets <- outlets |>
  filter(!domain %in% to_remove)

# Convert to BNG projection
outlets_sf <- outlets |>
  st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326)

metric_df <- readRDS("data/analysis_data.rds") |>
  ungroup() |>
  filter(!domain %in% to_remove) |>
  group_by(domain) |>
  summarise(num_locations = n(),
            num_articles = n_distinct(unique_article_id))

write_rds(metric_df, "data/features/metric_df.rds")
# ==============================================================================
# 1. SPATIAL EXTENT METRICS
# ==============================================================================
spatial_extent <- outlets_sf |>
  st_transform(27700) |>
  group_by(domain) |>
  arrange(desc(n)) |>
  group_modify(~ {
    # Find primary location
    primary_loc <- .x[which.max(.x$n), ]
    
    # Calculate cumulative mention proportions
    mention_df <- .x |>
      mutate(distance = as.numeric(st_distance(primary_loc, geometry)) / 1000) |>
      arrange(distance) |>
      mutate(cum_mentions = cumsum(n), cum_prop = cum_mentions / sum(n))
    
    threshold_df <- mention_df |>
      # Ensure at least 5 locations OR 75% coverage
      filter(cum_prop <= 0.75 | row_number() <= min(10, nrow(.x)))
    
    # Calculate metrics on this subset
    hull <- threshold_df |>
      summarise(geometry = st_union(geometry)) |>
      st_convex_hull()
    
    tibble(
      convex_hull_area = st_area(hull) |> as.numeric() / 1000000,
      radius_75_mentions = max(threshold_df$distance)
    )
  })

write_rds(spatial_extent, "data/features/spatial_extent.rds")

# ==============================================================================
# 2. ADMINISTRATIVE REACH METRICS
# ==============================================================================
# Import and combine boundaries for Scotland, Wales, NI, and England:
# Scotland: https://maps.gov.scot/ATOM/shapefiles/SG_DataZoneBdry_2022.zip
# Northern Ireland: https://www.nisra.gov.uk/files/nisra/publications/geography-sdz2021-esri-shapefile.zip
# Wales / England: https://geoportal.statistics.gov.uk/datasets/2bbaef5230694f3abae4f9145a3a9800_0/explore
# --------------------------------
# zip_files <- list.files("data/lsoa_boundaries/", pattern = "\\.zip$", full.names = TRUE)
#
# for(zip_file in zip_files) {
#   temp_dir <- tempdir()
#   unzip(zip_file, exdir = temp_dir)
#   shp_file <- list.files(temp_dir, pattern = "\\.shp$", full.names = TRUE, recursive = TRUE)[1]
#   shp <- st_read(shp_file, quiet = TRUE)
#   cat("File:", basename(zip_file), "CRS:", st_crs(shp)$input, "\n")
#   unlink(temp_dir, recursive = TRUE)
# }
#
# combined_boundaries <- list.files("data/lsoa_boundaries/",
#                                   pattern = "\\.zip$",
#                                   full.names = TRUE) |>
#   map_dfr(~{
#     temp_dir <- tempdir()
#     unzip(.x, exdir = temp_dir)
#     shp_file <- list.files(temp_dir, pattern = "\\.shp$", full.names = TRUE, recursive = TRUE)[1]
#     result <- st_read(shp_file, quiet = TRUE)
#     unlink(temp_dir, recursive = TRUE)
#
#     # Transform to WGS84 (EPSG:4326) - works for all UK/Ireland data
#     result <- st_transform(result, crs = 4326)
#
#     result |> mutate(source_file = basename(.x))
#   })
#
# saveRDS(combined_boundaries, "data/combined_boundaries.rds")
#------------------------------
# Attach boundaries to locations
#------------------------------
combined_boundaries <- readRDS("data/combined_boundaries.rds")
combined_boundaries <- st_make_valid(combined_boundaries) # Fix invalid geometries

# Create unified district name column
combined_boundaries <- combined_boundaries |>
  mutate(
    # Unified district NAME
    district_clean_nm = case_when(
      !is.na(SDZ2021_nm) ~ SDZ2021_nm,!is.na(LSOA21NM) ~ LSOA21NM,!is.na(DZName) ~ DZName,
      TRUE ~ NA_character_
    ),
    
    # Unified district CODE
    district_clean_cd = case_when(
      !is.na(SDZ2021_cd) ~ SDZ2021_cd,!is.na(LSOA21CD) ~ LSOA21CD,!is.na(DZCode) ~ DZCode,
      TRUE ~ NA_character_
    )
  )

combined_boundaries_small <- combined_boundaries[, 31:32]
outlets_proj <- st_transform(outlets_sf, 27700) 
districts_proj <- st_transform(combined_boundaries_small, 27700) 
nearest_indices <- st_nearest_feature(outlets_sf, combined_boundaries_small)
nearest_geoms <- st_geometry(combined_boundaries_small)[nearest_indices]
distances <- st_distance(st_geometry(outlets_sf), nearest_geoms, by_element = TRUE)
outlets_proj$dist_to_district <- as.numeric(distances)
locations_w_districts <- cbind(
  outlets_proj,  # retains sf class and geometry
  st_drop_geometry(districts_proj)[nearest_indices, ])

write_rds(locations_w_districts, "data/features/locations_w_district.rds")
# -------------------------------
# Import data
lsoac <- read_csv("data/lsoa_lad_data/LSOA_DZ_SDZ_Lookup.csv") # apply for access externally

admin_reach <- locations_w_districts |>
  left_join(lsoac, by = c("district_clean_cd" = "geography_code")) |>
  group_by(domain, district_clean_cd) |>
  summarise(n_mentions = sum(n), .groups = "drop") |>
  group_by(domain) |>
  summarise(
    n_lsoas = n_distinct(district_clean_cd),
    total_mentions = sum(n_mentions),
    mentions_per_lsoa = total_mentions / n_lsoas
  ) |>
  dplyr::select(-total_mentions)

write_rds(admin_reach, "data/features/admin_reach.rds")

names_codes <- read_csv("data/lsoa_lad_data/classification_codes_and_names_0-5.csv") |>
  filter(Level == "Supergroup")

names_codes$`Classification Code` <- as.integer(names_codes$`Classification Code`)

demographic_reach <- locations_w_districts |>
  st_drop_geometry(geometry) |>
  left_join(lsoac, by = c("district_clean_cd" = "geography_code")) |>
  left_join(names_codes, by = c("Supergroup" = "Classification Code")) |>
  group_by(domain, Supergroup, `Classification Name`) |>
  summarise(n_mentions = sum(n), .groups = "drop") |>
  group_by(domain) |>
  mutate(
    total_mentions = sum(n_mentions),
    pct_mentions = 100 * n_mentions / total_mentions
  ) |>
  arrange(domain, desc(pct_mentions)) |>
  ungroup() |>
  dplyr::select(domain, `Classification Name`, pct_mentions) |>
  pivot_wider(names_from = `Classification Name`,
              values_from = pct_mentions,
              values_fill = 0)     

write_rds(demographic_reach, "data/features/demographic_reach.rds")

diversity_metrics_codes <- locations_w_districts |>
  st_drop_geometry() |>
  left_join(lsoac, by = c("district_clean_cd" = "geography_code")) |>
  left_join(names_codes, by = c("Supergroup" = "Classification Code")) |>
  group_by(domain, Supergroup, `Classification Name`) |>
  summarise(n_mentions = sum(n), .groups = "drop") |>
  group_by(domain) |>
  mutate(p = n_mentions / sum(n_mentions)) |>
  summarise(gini = ineq::Gini(n_mentions)) 
# ==============================================================================
# 3. SPATIAL HETEROGENEITY
# ==============================================================================
diversity_metrics <- locations_w_districts |>
  st_drop_geometry() |> 
  group_by(domain, district_clean_cd) |>
  summarise(n = sum(n), .groups = "drop") |>
  group_by(domain) |>
  mutate(p = n / sum(n)) |>
  summarise(shannon = -sum(p * log(p + 1e-10))) 

combined_metrics <- diversity_metrics |> 
  rename(shannon_geo = shannon) |> 
  left_join(diversity_metrics_codes |> 
              rename(gini_demo = gini), 
            by = "domain")

write_rds(combined_metrics, "data/features/diversity_metrics.rds")

# Moran's I with 50km threshold and minimum 3 neighbors
morans_50 <- outlets_sf |>
  ungroup() |>
  group_by(domain) |>
  group_modify(~ {
    coords <- st_coordinates(.x)
    n_locs <- nrow(coords)
    
    # Create distance-based weights with 50km threshold
    dists <- distm(coords, fun = distHaversine) / 1000
    W <- (dists <= 50 &
            dists > 0) * 1  # 50km in meters
    
    # Ensure minimum 3 neighbors
    for (i in 1:n_locs) {
      if (sum(W[i, ]) < 3) {
        nearest <- order(dists[i, ])[2:4]  # Next 3 closest
        W[i, nearest] <- 1
        W[nearest, i] <- 1
      }
    }
    
    # Convert to spatial weights list
    weights <- mat2listw(W, style = "W")
    moran_result <- moran.test(.x$n, weights)
    
    tibble(morans_i = moran_result$estimate["Moran I statistic"], moran_p = moran_result$p.value)
  })

write_rds(morans_50, "data/features/morans_i.rds")
# ==============================================================================
# 4. DISTANCE DECAY METRICS
# ==============================================================================
outlets_with_distances <- outlets |>
  uncount(n, .id = "copy_id") |> # Expand rows according to weights (n = mention counts)
  mutate(distance_from_outlet = distHaversine(cbind(Longitude, Latitude), cbind(outlet_lon, outlet_lat)) / 1000)   # Calculate distances (in km) from outlet coordinates

write_rds(outlets_with_distances, "data/features/outlets_with_distances.rds")

dispersion_stats <- outlets_with_distances |>
  group_by(domain) |>
  summarise(
    mean_distance_km = mean(distance_from_outlet),
    median_distance_km = median(distance_from_outlet),
    sd_distance = sd(distance_from_outlet),
    max_distance_km = max(distance_from_outlet),
    cv_distance = sd_distance / mean_distance_km,
    pct_within_10km = mean(distance_from_outlet <= 10), 
    .groups = "drop"
  )

write_rds(dispersion_stats, "data/features/dispersion_stats.rds")
# ==============================================================================
# FINAL METRICS COMBINATION
# ==============================================================================
metrics <- read_rds("data/features/metric_df.rds")
spatial_extent <- read_rds("data/features/spatial_extent.rds")
admin_reach <- read_rds("data/features/admin_reach.rds")
diversity_metrics <- read_rds("data/features/diversity_metrics.rds")
morans_i <- read_rds("data/features/morans_i.rds")
dispersion_stats <- read_rds("data/features/dispersion_stats.rds")

metrics <- metrics |>
  left_join(spatial_extent, by = "domain") |>
  left_join(admin_reach, by = "domain") |>
  left_join(diversity_metrics, by = "domain") |>
  left_join(morans_i, by = "domain") |>
  left_join(dispersion_stats, by = "domain")

write_rds(metrics, "data/features/metrics.rds")

metrics_simple <- metrics |> select(-domain, -num_articles, -num_locations, -moran_p, -mean_distance_km, -sd_distance, -max_distance_km, -mentions_per_lsoa, -geometry)

colnames(metrics_simple) <- c(
  "Area",
  "Radius",
  "Districts",
  "Entropy",
  "Gini",
  "MoranI",
  "MedianDist",
  "DistCV",
  "Pct10km"
)

# Manuscript Table 2
library(stargazer)
stargazer(
  as.data.frame(metrics_simple),
  summary.stat = c("mean", "median", "sd", "min", "max"),
  title = "Descriptive Statistics",
  label = "tab:descriptives",
  out = "descriptives.tex",
  float = TRUE,
  header = FALSE,
  digits = 2
)
