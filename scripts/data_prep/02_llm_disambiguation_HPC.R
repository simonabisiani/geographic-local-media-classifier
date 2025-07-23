suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(purrr)
})

# Parse arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript process_batch.R <batch_file> <ollama_port>")
}

batch_id <- as.integer(args[1])
ollama_port <- as.integer(args[2])
ollama_host <- Sys.getenv("OLLAMA_HOST", "localhost")

# Configuration - FIXED variable names
ollama_url <- sprintf("http://%s:%d/api/generate", ollama_host, ollama_port)
batch_file <- sprintf("data/subbatches/batch_%d.rds", batch_id)  # Fixed: added underscore


# Configuration
output_folder <- "results"
if (!dir.exists(output_folder)) dir.create(output_folder)

# Simple logging
log_msg <- function(...) {
  cat(sprintf("[%s] Batch %d: %s\n",
              format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
              batch_id,
              paste(...)))
}

# Process and parse a single prompt
process_prompt <- function(prompt, unique_article_id) {
  response <- tryCatch({
    res <- POST(
      ollama_url,
      body = list(
        model = "gemma2:latest",
        prompt = prompt,
        stream = FALSE,
        format = "json",
        options = list(
          temperature = 0,
          seed = 42
        )
      ),
      encode = "json",
      timeout(60)
    )
    if (status_code(res) == 200) content(res)$response else NA_character_
  }, error = function(e) {
    log_msg("Error:", e$message)
    NA_character_
  })

if (!is.na(response)) {
    parsed <- tryCatch(fromJSON(response), error = function(e) NULL)
    list(
      unique_article_id = unique_article_id,
      prompt = prompt,  # Added this line
      raw_response = response,
      chosen_option = parsed$chosen_option %||% NA_character_,
      reasoning = parsed$reasoning %||% parsed$explanation %||% NA_character_,
      parse_success = !is.null(parsed)
    )
  } else {
    list(
      unique_article_id = unique_article_id,
      prompt = prompt,  # Added this line
      raw_response = NA_character_,
      chosen_option = NA_character_,
      reasoning = NA_character_,
      parse_success = FALSE
    )
  }
}

# Main processing
# Main processing
log_msg("Starting processing")
data <- readRDS(batch_file)
n_prompts <- nrow(data)
chunk_size <- 100


# Check for existing chunks and determine where to start
existing_chunks <- list.files(output_folder, 
                             pattern = sprintf("batch_%d_chunk_", batch_id), 
                             full.names = FALSE)

if (length(existing_chunks) > 0) {
  # Extract chunk numbers from filenames
  chunk_numbers <- as.numeric(gsub(sprintf("batch_%d_chunk_(\\d+)\\.rds", batch_id), "\\1", existing_chunks))
  last_chunk <- max(chunk_numbers)
  start_prompt <- (last_chunk - 1) * chunk_size + 1
  
  log_msg(sprintf("Found %d existing chunks. Last chunk: %d. Resuming from prompt %d", 
                  length(existing_chunks), last_chunk, start_prompt))
} else {
  start_prompt <- 1
  log_msg("No existing chunks found. Starting from prompt 1")
}

# Only process remaining prompts
if (start_prompt <= n_prompts) {
  results <- vector("list", n_prompts)
  save_counter <- 0
  
  for (i in start_prompt:n_prompts) {
    unique_article_id <- data$unique_article_id[i]
    results[[i]] <- process_prompt(data$prompt[i], unique_article_id)
    save_counter <- save_counter + 1
    
    # Save results every 100 prompts
    if (save_counter >= chunk_size || i == n_prompts) {
      chunk_file <- file.path(output_folder, sprintf("batch_%d_chunk_%d.rds", batch_id, ceiling(i / chunk_size)))
      
      # Get the range of results for this chunk
      chunk_start <- i - save_counter + 1
      chunk_results <- bind_rows(results[chunk_start:i])
      
      saveRDS(chunk_results, chunk_file)
      log_msg(sprintf("Saved %d prompts to: %s", save_counter, chunk_file))
      save_counter <- 0
    }
  }
} else {
  log_msg("All prompts already processed!")
}