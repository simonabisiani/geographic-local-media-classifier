library(purrr)
library(stringr)
library(stringdist)
library(readr)
library(dplyr)
library(DBI)
library(RSQLite)

############################################################
# Get unique LLM response
############################################################
setwd("/parallel_scratch/sb02767/llm_inference")
dir.create("unique_list_llm_answer", showWarnings = FALSE)

# Process .rds files
rds_files <- list.files("results_combined2", pattern = "\\.rds$", full.names = TRUE)
rds_data <- map_dfr(rds_files, readRDS)
unique_rds <- rds_data |> 
  distinct(chosen_option) |>
  write_rds("unique_list_llm_answer/unique_from_rds.rds")

# Process .db files
db_files <- list.files("db_files", pattern = "\\.db$", full.names = TRUE)
db_data <- map_dfr(db_files, function(db_path) {
  con <- dbConnect(SQLite(), db_path)
  data <- dbReadTable(con, "results")
  dbDisconnect(con)
  data |> 
    distinct(chosen_option) |>
    mutate(batch = tools::file_path_sans_ext(basename(db_path)))
})
write_rds(db_data, "unique_list_llm_answer/unique_from_db.rds")

# Combine and deduplicate
all_unique <- bind_rows(
  read_rds("unique_list_llm_answer/unique_from_rds.rds"),
  read_rds("unique_list_llm_answer/unique_from_db.rds") |> select(chosen_option)
) |> 
  distinct(chosen_option)

write_rds(all_unique, "unique_list_llm_answer/all_unique_chosen_options.rds")

############################################################
# Investigate LLM responses (manual step)
############################################################
# Manual inspection and creation of mapping files:
# - LAD_name_cleaning.rds: tibble(chosen_option, LAD24NM)
# - LAD_chosen_option_to_remove.rds: tibble(chosen_option, to_remove = TRUE)
# 1. Load all unique chosen options
all_unique <- read_rds("unique_list_llm_answer/all_unique_chosen_options.rds")

# 2. Load official LAD names (assuming you have a reference file)
official_lads <- googlesheets4::read_sheet(ss = "https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1316575984#gid=1316575984",
                                           sheet = "Districts") |> 
  pull(LAD) |> 
  unique()

# Create initial categorization
categorized <- all_unique |>
  mutate(
    # Exact matches
    exact_match = chosen_option %in% official_lads,
    
    # Multiple LADs (contains semicolon)
    multiple_lads = str_detect(chosen_option, ";"),
    
    # Potential variations (fuzzy matching)
    closest_match = map_chr(chosen_option, function(x) {
      if (x %in% official_lads) return(x)
      
      distances <- stringdist(x, official_lads, method = "jw")
      
      # Ensure distances are valid
      if (all(is.na(distances))) {
        return(NA_character_)
      }
      
      # Find the closest match if the minimum distance is acceptable
      if (min(distances, na.rm = TRUE) < 0.2) {
        official_lads[which.min(distances)]
      } else {
        NA_character_
      }
    }),
    
    # Flag obvious non-LADs
    likely_remove = str_detect(chosen_option, regex(
      "not found|unknown|n/a|none|unclear|multiple|various", 
      ignore_case = TRUE
    ))
  )

# 4. Interactive review helper
review_mappings <- function(df) {
  # Group by similarity for easier review
  df |>
    arrange(
      desc(likely_remove),
      desc(multiple_lads),
      desc(exact_match),
      chosen_option
    ) |>
    mutate(
      review_group = case_when(
        exact_match ~ "1_exact",
        !is.na(closest_match) ~ "2_fuzzy",
        multiple_lads ~ "3_multiple",
        likely_remove ~ "4_remove",
        TRUE ~ "5_manual"
      )
    )
}

reviewed <- categorized |> review_mappings()

# # 5. Export for manual review in Excel/CSV
# googlesheets4::write_sheet(reviewed, ss = "https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1316575984#gid=1316575984",
#                            sheet="chosen_options_manual_clean")


############################################################
# Review new data
############################################################

# Function to find new unique options
find_new_options <- function(new_results, existing_mapping, existing_removals) {
  # Get all previously processed options
  already_processed <- c(
    existing_mapping$chosen_option,
    existing_removals$chosen_option
  )
  
  # Extract unique options from new results
  new_unique <- new_results |>
    pull(chosen_option) |>
    unique()
  
  # Find truly new options
  truly_new <- setdiff(new_unique, already_processed)
  
  tibble(chosen_option = truly_new)
}

# Load existing mappings
existing_mapping <- read_rds("LAD_name_cleaning.rds")
existing_removals <- read_rds("LAD_chosen_option_to_remove.rds")


# Find new options that need review
new_options <- find_new_options(all_unique, existing_mapping, existing_removals)

if (nrow(new_options) > 0) {
  cat("Found", nrow(new_options), "new unique options to review\n")
  
  # Run the same categorization process on just the new options
  new_categorized <- new_options |>
    mutate(
      # Exact matches
      exact_match = chosen_option %in% official_lads,
      
      # Multiple LADs (contains semicolon)
      multiple_lads = str_detect(chosen_option, ";"),
      
      # Potential variations (fuzzy matching)
      closest_match = map_chr(chosen_option, function(x) {
        if (x %in% official_lads) return(x)
        
        distances <- stringdist(x, official_lads, method = "jw")
        
        # Ensure distances are valid
        if (all(is.na(distances))) {
          return(NA_character_)
        }
        
        # Find the closest match if the minimum distance is acceptable
        if (min(distances, na.rm = TRUE) < 0.2) {
          official_lads[which.min(distances)]
        } else {
          NA_character_
        }
      }),
      
      # Flag obvious non-LADs
      likely_remove = str_detect(chosen_option, regex(
        "not found|unknown|n/a|none|unclear|multiple|various", 
        ignore_case = TRUE
      ))
    )
  
  # Review and categorize
  new_reviewed <- new_categorized |> review_mappings()
  
  # Append to Google Sheets (or create a new sheet)
  googlesheets4::sheet_append(
    ss = "https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1316575984#gid=1316575984",
    data = new_reviewed,
    sheet = "chosen_options_manual_clean"
  )
  
} else {
  cat("No new unique options found!\n")
}

############################################################
# After manual review, import and create final file
############################################################
reviewed_manual <- googlesheets4::read_sheet(ss = "https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1316575984#gid=1316575984",
                                             sheet="chosen_options_manual_clean")

# Create mapping file
lad_mapping <- reviewed_manual |>
  filter(!is.na(LAD24NM_final)) |>
  select(chosen_option, LAD24NM = LAD24NM_final)

# Create removal file
to_remove <- reviewed_manual |>
  filter(is.na(LAD24NM_final)) 

# Save
write_rds(lad_mapping, "LAD_name_cleaning.rds")
write_rds(to_remove, "LAD_chosen_option_to_remove.rds")

############################################################
# Clean data
############################################################

# Function to clean results
clean_results <- function(results_data) {
  results_data |>
    left_join(lad_mapping, by = "chosen_option") |>
    mutate(chosen_LAD24NM = coalesce(LAD24NM, chosen_option)) |>
    anti_join(to_remove, by = "chosen_option") |>
    select(-LAD24NM)
}

# Process all .rds files
rds_files <- list.files("/parallel_scratch/sb02767/llm_inference/results_combined2", pattern = "\\.rds$", full.names = TRUE)
rds_cleaned <- map_dfr(rds_files, ~readRDS(.x) |> clean_results()) %>% distinct()

# Process all .db files
db_files <- list.files("db_files", pattern = "\\.db$", full.names = TRUE)
db_cleaned <- map_dfr(db_files, function(db_path) {
  con <- dbConnect(SQLite(), db_path)
  data <- dbReadTable(con, "results")
  dbDisconnect(con)
  clean_results(data)
})

# Combine everything
all_results_cleaned <- bind_rows(rds_cleaned %>% 
                                   select(c(prompt, chosen_LAD24NM)), 
                                 db_cleaned %>% 
                                   select(c(prompt, chosen_LAD24NM))) %>% distinct()

# Save Parquet (excellent for large data, cross-language) library(arrow)
write_rds(all_results_cleaned, "all_results_cleaned.rds")

############################################################
# Attach spatial data
############################################################
coordinates <- read_csv("../csv/toponym_candidates_with_lads_no_dupl_lads.csv")
prompts_df <- read_rds("prompts_with_mapping.rds")

results_w_articles <- all_results_cleaned %>% 
  left_join(prompts_df, by = "prompt")

results_w_coordinates <- results_w_articles %>%
  mutate(values = str_to_lower(value)) %>% 
  inner_join(coordinates, by = c("values", "chosen_LAD24NM" = "LAD24NM"))

# Resolved Locations
nrow(results_w_coordinates)/nrow(prompts_df)*100

############################################################
# Attach article metadata
############################################################
outlet_loc <- googlesheets4::read_sheet(ss="https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1038895799#gid=1038895799",
                                        sheet="domains_w_coverage_geocoded") %>% 
  select(c(domain, primary_location, lat, lon))

# Load data
articles <- 
  read_csv("../csv/articles_sample.csv") |>
  mutate(main_LAD = if_else(is.na(main_LAD), LAD, main_LAD))

articles_w_outlet_loc <- articles %>% left_join(outlet_loc %>% 
                                                  rename(outlet_lat = lat, outlet_lon = lon), 
                                                by = "domain")
# unique_articles <- 
#   articles |>
#   mutate(id_dupl = if_else(is.na(duplicate_group), article_id, duplicate_group)) |>
#   group_by(id_dupl) |>
#   slice_min(order_by = tweet_date, n = 1)

# Join with outlet metadata
analysis_data <- results_w_coordinates %>%
  left_join(articles_w_outlet_loc, by = c("article_text", "unique_article_id"))

(analysis_data %>% select(article_text, unique_article_id) %>% n_distinct())/nrow(articles)
# 0.7466938 articles from sample with locations

# unique locations
analysis_data |> select(value) |> n_distinct() 
# 158288

saveRDS(analysis_data %>% distinct(), "analysis_data.rds")
# write_rds(analysis_data %>% filter(domain %in% c("wiltsglosstandard.co.uk", "keighleynews.co.uk")), "analysis_data_sample.rds")

# analysis_data <- readRDS("analysis_data.rds")
analysis_small <- analysis_data %>% 
  select(domain, Latitude, Longitude, outlet_lat, outlet_lon) %>% 
  group_by(domain, Latitude, Longitude, outlet_lat, outlet_lon) %>%
  count()

write_rds(analysis_small, "analysis_small.rds")
