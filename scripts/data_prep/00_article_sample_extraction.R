# Stratified Cluster Sampling with Constructed Weeks
# Two-dimensional stratification by LAD-publisher combinations
# Temporal clustering using constructed weeks
library(dplyr)
library(lubridate)
library(readr)
library(stringr)
library(ggplot2)
library(tidyr)
library(gt)

# articles.csv is downloadable here, from articles.zip: https://dataverse.harvard.edu/citation?persistentId=doi:10.7910/DVN/R5XTEO
articles <- read_csv("data/articles.csv")  
articles <- articles |> mutate(date = str_remove(tweet_date, ";.*"))

# =============================================================================
# STRATIFIED CLUSTER SAMPLING FUNCTION
# =============================================================================

stratified_cluster_sample <- function(articles, 
                                      weeks_per_year,
                                      seed) {
  
  set.seed(seed)
  
  # Prepare data with temporal variables
  articles_prep <- articles |>
    mutate(
      date = as.Date(date),
      year = year(date),
      quarter = quarter(date),
      day_of_week = wday(date, label = TRUE),
      week_of_year = week(date)
    )
  
  # Get unique years in dataset
  years <- sort(unique(articles_prep$year))
  
  # ==========================================================================
  # STEP 1: IDENTIFY LAD-PUBLISHER STRATA
  # ==========================================================================
  # Create strata based on existing combinations
  strata <- articles_prep |>
    group_by(LAD, owner) |>
    summarise(
      total_articles = n(),
      first_date = min(date),
      last_date = max(date),
      n_years_active = n_distinct(year),
      .groups = 'drop'
    ) |>
    arrange(LAD, desc(total_articles))
  
  # ==========================================================================
  # STEP 2: CREATE CONSTRUCTED WEEKS FOR EACH STRATUM-YEAR
  # ==========================================================================
  # Function to create constructed weeks for a specific stratum-year
  create_constructed_weeks <- function(stratum_data, n_weeks = weeks_per_year) {
    
    # Calculate weeks per quarter (evenly distributed)
    weeks_per_quarter <- n_weeks / 4
    
    constructed_weeks <- data.frame()
    
    for (q in 1:4) {
      quarter_data <- stratum_data |> filter(quarter == q)
      
      if (nrow(quarter_data) == 0) next
      
      # Create constructed weeks for this quarter
      for (week_num in 1:weeks_per_quarter) {
        
        # Sample one day from each day of week in this quarter
        week_sample <- quarter_data |>
          group_by(day_of_week) |>
          slice_sample(n = 1) |>
          ungroup() |>
          mutate(
            constructed_week = paste0("CW_Q", q, "_W", week_num),
            stratum_id = paste(first(LAD), first(owner), first(year), sep = "_")
          )
        
        constructed_weeks <- rbind(constructed_weeks, week_sample)
      }
    }
    
    return(constructed_weeks)
  }
  
  # Apply constructed week sampling to each stratum-year combination
  sampled_days <- data.frame()
  
  for (i in 1:nrow(strata)) {
    
    current_LAD <- strata$LAD[i]
    current_owner <- strata$owner[i]
    
    cat("Processing stratum", i, "of", nrow(strata), ":", current_LAD, "-", current_owner, "\n")
    
    # Get data for this stratum across all years
    stratum_data <- articles_prep |>
      filter(LAD == current_LAD, owner == current_owner)
    
    # Create constructed weeks for each year this stratum has data
    for (yr in years) {
      
      year_data <- stratum_data |> filter(year == yr)
      
      if (nrow(year_data) == 0) next
      
      # Create constructed weeks for this stratum-year
      stratum_year_sample <- create_constructed_weeks(year_data, weeks_per_year)
      
      sampled_days <- rbind(sampled_days, stratum_year_sample)
    }
  }

  # ==========================================================================
  # STEP 3: EXTRACT FINAL SAMPLE (CENSUS OF ARTICLES ON SAMPLED DAYS)
  # ==========================================================================
  # Create sampling frame with metadata
  sampled_dates <- sampled_days |>
    select(LAD, owner, date, constructed_week, stratum_id) |>
    distinct()
  
  final_sample <- articles_prep |>
    inner_join(sampled_dates, by = c("LAD", "owner", "date"), relationship = "many-to-many") |>
    distinct() |>  # Remove completely duplicate rows
    mutate(sampling_method = "stratified_cluster")

   # ==========================================================================
  # STEP 4: SAMPLE VALIDATION AND SUMMARY
  # ==========================================================================
  # Temporal representation
  temporal_summary <- final_sample |>
    group_by(year, quarter) |>
    summarise(
      n_articles = n(),
      n_days = n_distinct(date),
      n_strata = n_distinct(stratum_id),
      .groups = 'drop'
    )
  
  print(temporal_summary)
  
  # Strata representation
  strata_summary <- final_sample |>
    group_by(LAD, owner) |>
    summarise(
      n_articles = n(),
      n_days = n_distinct(date),
      n_weeks = n_distinct(constructed_week),
      articles_per_day = round(n_articles / n_days, 1),
      .groups = 'drop'
    ) |>
    arrange(desc(n_articles))
  
  print(head(strata_summary, 10))
  
  # Publisher representation
  publisher_summary <- final_sample |>
    count(owner, name = "sampled_articles") |>
    left_join(
      articles_prep |> count(owner, name = "total_articles"),
      by = "owner"
    ) |>
    mutate(
      sampling_rate = round(sampled_articles / total_articles * 100, 2),
      sample_pct = round(sampled_articles / sum(sampled_articles) * 100, 1)
    ) |>
    arrange(desc(sampled_articles))
  
  print(head(publisher_summary, 10))
  
  # Geographic representation
  geographic_summary <- final_sample |>
    count(LAD, name = "sampled_articles") |>
    left_join(
      articles_prep |> count(LAD, name = "total_articles"),
      by = "LAD"
    ) |>
    mutate(
      sampling_rate = round(sampled_articles / total_articles * 100, 2),
      sample_pct = round(sampled_articles / sum(sampled_articles) * 100, 1)
    ) |>
    arrange(desc(sampled_articles))
  
  print(head(geographic_summary, 10))
  
  return(final_sample)
}

# =============================================================================
# RUN THE SAMPLING FUNCTION
# =============================================================================

articles_sample <- stratified_cluster_sample(articles, weeks_per_year = 8, seed = 2010)
set.seed(124)
articles_sample <- articles_sample[!duplicated(articles_sample$unique_article_id), ]
# write_csv(articles_sample, "data/articles_sample.csv")

# =============================================================================
# DESCRIPTIVE STATISTICS
# =============================================================================
# Load the sample (pre-saved)
articles_sample <- read_csv("data/articles_sample.csv")

# Population parameters from provided statistics
total_articles <- 2534705
total_domains <- 360
total_publishers <- 87
total_lads <- 337

# Basic counts
basic_stats <- tibble(
  Metric = c(
    "Total articles (sample)",
    "Total articles (population)",
    "Sampling rate (%)",
    "Number of outlets (sample)",
    "Number of outlets (population)",
    "Number of LADs (sample)",
    "Number of LADs (population)",
    "Number of publishers (sample)",
    "Number of publishers (population)",
    "Number of sampled days",
    "Articles per sampled day (mean)"
  ),
  Value = c(
    format(nrow(articles_sample), big.mark = ","),
    format(total_articles, big.mark = ","),
    sprintf("%.2f", nrow(articles_sample) / total_articles * 100),
    format(n_distinct(articles_sample$domain), big.mark = ","),
    format(total_domains, big.mark = ","),
    n_distinct(articles_sample |> select(LAD) |> separate_rows(LAD, sep = "; ")),
    total_lads,
    n_distinct(articles_sample$owner),
    total_publishers,
    n_distinct(articles_sample$date),
    sprintf("%.1f", nrow(articles_sample) / n_distinct(articles_sample$date))
  )
)

# Temporal distribution
temporal_stats <- articles_sample %>%
  group_by(year) %>%
  summarise(
    Articles = format(n(), big.mark = ","),
    Days = n_distinct(date),
    LADs = n_distinct(main_LAD),
    Publishers = n_distinct(owner)
  )

# Publisher concentration
top_publishers <- articles_sample %>%
  count(owner, sort = TRUE) %>%
  mutate(
    percent = n / sum(n) * 100,
    cumulative_percent = cumsum(percent)
  ) %>%
  slice_head(n = 5) %>%
  mutate(
    Articles = format(n, big.mark = ","),
    Percent = sprintf("%.1f%%", percent),
    Cumulative = sprintf("%.1f%%", cumulative_percent)
  ) %>%
  select(Publisher = owner, Articles, Percent, Cumulative)

# Geographic coverage
geographic_coverage <- articles_sample %>%
  group_by(main_LAD) %>%
  summarise(n_articles = n()) %>%
  summarise(
    `LADs with <100 articles` = sum(n_articles < 100),
    `LADs with 100-500 articles` = sum(n_articles >= 100 & n_articles < 500),
    `LADs with 500-1000 articles` = sum(n_articles >= 500 & n_articles < 1000),
    `LADs with >1000 articles` = sum(n_articles >= 1000)
  ) %>%
  pivot_longer(everything(), names_to = "Category", values_to = "Count")

# Create formatted tables
print("Table 1: Sample Overview")
print(basic_stats)

print("\nTable 2: Temporal Distribution")
print(temporal_stats)

print("\nTable 3: Top 5 Publishers in Sample")
print(top_publishers)

print("\nTable 4: Geographic Coverage")
print(geographic_coverage)

# Export as LaTeX table (example for main table)
basic_stats %>%
  gt() %>%
  tab_header(title = "Descriptive Statistics of Article Sample") %>%
  as_latex() %>%
  writeLines("output/tables/sample_statistics.tex")