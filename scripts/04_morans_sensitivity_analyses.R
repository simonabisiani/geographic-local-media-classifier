# Moran's I with 25km threshold and minimum 3 neighbors
morans_25 <- outlets_sf |>
  ungroup() |>
  group_by(domain) |>
  group_modify(~ {
    coords <- st_coordinates(.x)
    n_locs <- nrow(coords)
    
    # Create distance-based weights with 50km threshold
    dists <- distm(coords, fun = distHaversine) / 1000
    W <- (dists <= 25 &
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

# Moran's I with 75km threshold and minimum 3 neighbors
morans_75 <- outlets_sf |>
  ungroup() |>
  group_by(domain) |>
  group_modify(~ {
    coords <- st_coordinates(.x)
    n_locs <- nrow(coords)
    
    # Create distance-based weights with 50km threshold
    dists <- distm(coords, fun = distHaversine) / 1000
    W <- (dists <= 75 &
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

# Combine results
morans_comparison <- morans_25 %>%
  rename(morans_25 = morans_i, pval_25 = moran_p) %>%
  left_join(morans_50 %>% rename(morans_50 = morans_i, pval_50 = moran_p), by = "domain") %>%
  left_join(morans_75 %>% rename(morans_75 = morans_i, pval_75 = moran_p), by = "domain")

# Basic descriptive stats
summary(morans_comparison[c("morans_25", "morans_50", "morans_75")])

# Value correlations
cor(morans_comparison[c("morans_25", "morans_50", "morans_75")], use = "complete.obs")

# Rank correlations (more robust)
cor(morans_comparison[c("morans_25", "morans_50", "morans_75")], 
    method = "spearman", use = "complete.obs")

# Create significance flags
morans_comparison <- morans_comparison %>%
  mutate(
    sig_25 = pval_25 < 0.05,
    sig_50 = pval_50 < 0.05,
    sig_75 = pval_75 < 0.05
  )

# Cross-tabulation of significance
table(morans_comparison$sig_25, morans_comparison$sig_50)
table(morans_comparison$sig_25, morans_comparison$sig_75)
table(morans_comparison$sig_50, morans_comparison$sig_75)

# Scatterplot matrix
pairs(morans_comparison[c("morans_25", "morans_50", "morans_75")])

# Or identify outlets with biggest changes
morans_comparison %>%
  mutate(range = pmax(morans_25, morans_50, morans_75) - pmin(morans_25, morans_50, morans_75)) %>%
  arrange(desc(range))
