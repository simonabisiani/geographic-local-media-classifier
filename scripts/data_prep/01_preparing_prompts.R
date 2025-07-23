library(dplyr)
library(purrr)
library(readr)
library(tidyr)
library(data.table)
library(jsonlite)
library(googlesheets4)

############################################################
# articles sample created from UKTwitNewsCor
############################################################
articles <- 
  read_csv("data/articles_sample.csv") |>
  mutate(main_LAD = if_else(is.na(main_LAD), LAD, main_LAD))

unique_articles <- 
  articles |>
  mutate(id_dupl = if_else(is.na(duplicate_group), 
                           article_id, duplicate_group)) |>
  group_by(id_dupl) |>
  slice_min(order_by = tweet_date, n = 1)

unique_articles_id <- unique_articles %>% ungroup() %>% select(unique_article_id)

m1_simple <- fread("arts_ents.csv")
m1_no_org <- m1_simple |>
  filter(variable != 'ORG')

rm(m1_simple)

############################################################
# candidates
############################################################
toponym_candidates_with_lads <- 
  read_csv("toponym_candidates_with_lads.csv")

toponym_candidates_with_lads_no_dupl_lads <- 
  read_csv("toponym_candidates_with_lads_no_dupl_lads.csv")

############################################################
# attach candidates to articles
############################################################
merged_art_ents_cands <- m1_no_org |>
  inner_join(unique_articles_id, by = "unique_article_id") %>% 
  mutate(entity = stringr::str_to_lower(value)) |>
  left_join(toponym_candidates_with_lads_no_dupl_lads, by = c('entity' = 'values'))

merged_art_ents_cands_dupl <- m1_no_org |>
  inner_join(unique_articles_id, by = "unique_article_id") %>% 
  mutate(entity = stringr::str_to_lower(value)) |>
  left_join(toponym_candidates_with_lads, by = c('entity' = 'values'))

library(ggplot2)
duplication_stats <- merged_art_ents_cands_dupl |>
  filter(is.na(LAD24NM)) |>
  group_by(value, LAD24NM) |>
  summarise(n_coords = n(), .groups = 'drop') |>
  group_by(value) |>
  summarise(has_dup_lad = any(n_coords > 1), .groups = 'drop')

# Calculate the proportion
n_total_entities <- nrow(duplication_stats)
n_entities_with_dup_lads <- sum(duplication_stats$has_dup_lad)
percentage_with_dup_lads <- (n_entities_with_dup_lads / n_total_entities) * 100
cat(sprintf("Percentage of entities with multiple candidates in the same LAD: %.2f%%\n", percentage_with_dup_lads))

no_locs <- anti_join(articles, merged_art_ents_cands %>% select(unique_article_id) %>% distinct(), by = "unique_article_id") 

# filter out articles without any entities
merged_art_ents_cands_w_lad <- merged_art_ents_cands |>
  filter(!is.na(LAD24NM)) 

n_tasks_per_article <- merged_art_ents_cands_w_lad |>
  ungroup() |>
  select(c(tweet_date, domain, main_LAD, unique_article_id, value)) |>
  group_by(unique_article_id) |>
  mutate(n_entities = n_distinct(value)) |>
  ungroup() |>
  distinct()

article_text <- unique_articles |>
  select(article_text, unique_article_id)

write_csv(article_text, "article_text_and_id_sample.csv")

merged_art_ents_cands_w_lad1 <- merged_art_ents_cands_w_lad |>
  left_join(article_text, by = "unique_article_id")

############################################################
# create prompts
############################################################

prompt_ingredients <- merged_art_ents_cands_w_lad1 %>% 
  select(unique_article_id, article_text, domain, value, LAD24NM) %>% 
  group_by(unique_article_id, value) %>% 
  mutate(candidates = paste0(LAD24NM, collapse = "; ")) %>% 
  ungroup() %>% 
  select(-LAD24NM) %>%
  distinct()

write_csv(prompt_ingredients, "../../parallel_scratch/sb02767/prompt_ingredients.csv")

# Define the full prompt template
full_prompt_template <- "
The task is mapping an entity (a toponym) to the Local Authority District (LAD) in which it is situated. Your goal is to select the correct option from the list provided. Instructions:
1. Review Entity and Article:
   - Identify the toponym (location name).
   - Read the article carefully to understand the context.
   - Use surrounding text to infer the location.
2. Check Metadata where provided:
   - Domain: The publisher’s domain may provide geographic context.
3. Select answer from options:
   Choose the correct answer from the options based on context.
   Options:
   - A list of applicable Districts, if any.
   - 'LAD not in options' if the correct District is missing.
   - 'Entity is not a location' if applicable.
   - 'Entity is outside the UK' for non-UK locations.
   - 'Entity spans across several districts (e.g., a region)' for non-specific entities.
   - 'Unsure' if uncertain.
4. Generate Response:
   - Format your response as JSON:
     {
       'chosen_option': 'Your choice',
       'reasoning': 'Your reasoning'
     }
"

classification_question <- "Which of the options provided best represents the Local Authority District (LAD) for the entity provided, based on the context in the article? Ensure the response is STRICTLY in JSON format with NO ADDITIONAL TEXT, explanations, or commentary OUTSIDE OF THE JSON OBJECT. Match the JSON schema indicated. Example of output: {\'chosen_option\': \'Fife\', \'reasoning\': \'The article refers to a toponym situated in Fife.\'}"

# Return tibble with just prompts and models (minimal data frame)
prepare_prompts_minimal <- function(data) {
  tibble(
    prompt = paste0(
      full_prompt_template, "\n\n",
      "Entity: ", data$value, "\n\n",
      "Article: ", data$article_text, "\n\n", 
      "Domain: ", data$domain, "\n\n",
      "Options: ", data$candidates, "\n\n",
      classification_question
    ),
    model = "gemma2:latest",
    unique_article_id = data$unique_article_id
  )
}

# Pre-generate prompts
input_data <- prepare_prompts_minimal(prompt_ingredients) 
write_csv(input_data, "../../parallel_scratch/sb02767/prompts.csv")  

# Simplified function to split dataset into batch files
split_dataset_to_batches <- function(data, batch_size = 25000, output_dir = "../../parallel_scratch/sb02767/prompt_batches_3") {

  # Create output directory
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  total_rows <- nrow(data)
  total_batches <- ceiling(total_rows / batch_size)
  
  # Split and save batches
  batch_files <- map_chr(1:total_batches, function(i) {
    start_row <- (i - 1) * batch_size + 1
    end_row <- min(i * batch_size, total_rows)
    
    batch_data <- data[start_row:end_row, ]
    batch_file <- file.path(output_dir, paste0("batch_", sprintf("%04d", i), ".rds"))
    saveRDS(batch_data, batch_file)
    
    cat(sprintf("Created batch %d/%d (%d rows)\n", i, total_batches, nrow(batch_data)))
    return(batch_file)
  })
  gc()
  return(batch_files)
}

# Split into batches
#batch_files <- split_dataset_to_batches(prompt_ingredients)
split_dataset_to_batches(input_data)