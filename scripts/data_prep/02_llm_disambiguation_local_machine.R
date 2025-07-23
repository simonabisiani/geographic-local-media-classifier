library(tidyverse)
library(DBI)
library(RSQLite)
library(furrr)
library(httr2)
library(jsonlite)

query_llm <- function(data, api_url = "http://localhost:11434", max_retries = 3) {
  # Validate inputs
  if (!all(c("prompt", "model") %in% colnames(data))) {
    stop("Data must contain 'prompt' and 'model' columns.")
  }
  
  # Create httr2_request objects for each prompt
  reqs <- purrr::map2(data$prompt, data$model, function(prompt, model) {
    httr2::request(api_url) %>%
      httr2::req_url_path("/api/generate") %>%
      httr2::req_headers("Content-Type" = "application/json") %>%
      httr2::req_body_json(list(
        model = model,
        prompt = prompt,
        stream = FALSE,
        format = "json",
        keep_alive = "10s",
        options = list(seed = 42, temperature = 0)
      ))
  })
  
  # Helper to perform retries
  perform_with_retries <- function(req, retries) {
    for (i in seq_len(retries)) {
      resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
      if (!is.null(resp)) return(resp)
    }
    return(NULL)  # If all retries fail
  }
  
  # Make parallel requests with retries
  resps <- purrr::map(reqs, ~ perform_with_retries(.x, max_retries))
  
  # Process the responses
  results <- purrr::map(resps, function(resp) {
    if (is.null(resp)) return(NULL)
    tryCatch(httr2::resp_body_json(resp)$response, error = function(e) NULL)
  })
  
  # Add results to the original data
  data$api_result <- results
  data$success <- !is.na(data$api_result)
  
  return(data)
}


# Parse annotations function
parse_annotations <- function(df) {
  processed_result <- df %>%
    mutate(
      parsed_json = map(api_result, ~ {
        if (is.na(.x) || is.null(.x)) return(NULL)
        tryCatch(fromJSON(.x), error = function(e) NULL)
      }),
      chosen_option = map_chr(parsed_json, ~ {
        if (is.null(.x)) return(NA_character_)
        as.character(.x$chosen_option %||% NA_character_)
      }),
      reasoning = map_chr(parsed_json, ~ {
        if (is.null(.x)) return(NA_character_)
        as.character(.x$reasoning %||% .x$explanation %||% NA_character_)
      })
    ) %>%
    select(-parsed_json)
  
  return(processed_result)
}

# Process a single batch with chunking
process_batch_with_chunks <- function(batch_file, batch_id, db_path, chunk_size = 25) {
  con <- dbConnect(RSQLite::SQLite(), db_path)
  on.exit(dbDisconnect(con))
  
  # Update progress to processing
  dbExecute(con, "
    UPDATE progress 
    SET status = 'processing', start_time = CURRENT_TIMESTAMP
    WHERE batch_id = ?
  ", params = list(batch_id))
  
  tryCatch({
    # Load batch data
    batch_data <- readRDS(batch_file)
    total_rows <- nrow(batch_data)
    
    # Ensure prompt column exists
    if (!"prompt" %in% colnames(batch_data)) {
      # Add your prepare_prompts_minimal function here if needed
      stop("No prompt column found in batch data")
    }
    
    # Process in chunks
    chunk_count <- ceiling(total_rows / chunk_size)
    
    for (chunk_idx in 1:chunk_count) {
      start_idx <- (chunk_idx - 1) * chunk_size + 1
      end_idx <- min(chunk_idx * chunk_size, total_rows)
      
      # Check if chunk already processed
      existing <- dbGetQuery(con, "
        SELECT COUNT(*) as count 
        FROM chunk_progress 
        WHERE batch_id = ? AND chunk_id = ? AND status = 'completed'
      ", params = list(batch_id, chunk_idx))
      
      if (existing$count > 0) {
        cat(sprintf("Skipping chunk %d/%d for batch %s (already processed)\n", 
                    chunk_idx, chunk_count, basename(batch_file)))
        next
      }
      
      # Record chunk start
      dbExecute(con, "
        INSERT OR REPLACE INTO chunk_progress 
        VALUES (?, ?, 'started', CURRENT_TIMESTAMP, NULL)
      ", params = list(batch_id, chunk_idx))
      
      # Process chunk
      chunk_data <- batch_data[start_idx:end_idx, ]
      
      processed_chunk <- chunk_data %>% 
        query_llm() %>% 
        parse_annotations()
      
      processed_chunk$batch_file <- batch_file
      processed_chunk$batch_id <- batch_id
      processed_chunk$chunk_id <- chunk_idx
      dbWriteTable(con, "results", 
                   processed_chunk %>% 
                     select(batch_id, batch_file, chunk_id,
                            prompt, model, chosen_option, reasoning, success),
                   append = TRUE)
      
      
      # Update chunk progress
      dbExecute(con, "
        UPDATE chunk_progress 
        SET status = 'completed', end_time = CURRENT_TIMESTAMP
        WHERE batch_id = ? AND chunk_id = ?
      ", params = list(batch_id, chunk_idx))
      
      cat(sprintf("Completed chunk %d/%d for batch %s\n", 
                  chunk_idx, chunk_count, basename(batch_file)))
      
      # Clean up memory
      rm(chunk_data, processed_chunk)
      gc()
    }
    
    # Update batch progress
    dbExecute(con, "
      UPDATE progress 
      SET status = 'completed', end_time = CURRENT_TIMESTAMP
      WHERE batch_id = ?
    ", params = list(batch_id))
    
    return(TRUE)
    
  }, error = function(e) {
    dbExecute(con, "
      UPDATE progress 
      SET status = 'failed', end_time = CURRENT_TIMESTAMP, error_message = ?
      WHERE batch_id = ?
    ", params = list(as.character(e), batch_id))
    return(FALSE)
  })
}

# Main parallel processing function
process_batch_files_parallel <- function(batch_files, 
                                         db_path = "llm_results.db",
                                         n_workers = parallel::detectCores() - 1,
                                         chunk_size = 25,
                                         resume = TRUE) {
  
  # Setup database
  con <- dbConnect(RSQLite::SQLite(), db_path)
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS results (
      rowid INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id INTEGER,
      batch_file TEXT,
      chunk_id INTEGER,
      prompt TEXT,
      model TEXT,
      chosen_option TEXT,
      reasoning TEXT,
      success INTEGER,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS progress (
      batch_id INTEGER PRIMARY KEY,
      batch_file TEXT,
      status TEXT,
      start_time DATETIME,
      end_time DATETIME,
      error_message TEXT
    )
  ")
  
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS chunk_progress (
      batch_id INTEGER,
      chunk_id INTEGER,
      status TEXT,
      start_time DATETIME,
      end_time DATETIME,
      PRIMARY KEY (batch_id, chunk_id)
    )
  ")
  
  # Create indices for better performance
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_results_batch ON results(batch_id)")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_chunk_progress ON chunk_progress(batch_id, chunk_id)")
  
  # Initialize or update progress table
  for (i in seq_along(batch_files)) {
    batch_file <- batch_files[i]
    existing <- dbGetQuery(con, "
      SELECT status FROM progress WHERE batch_id = ?
    ", params = list(i))
    
    if (nrow(existing) == 0) {
      dbExecute(con, "
        INSERT INTO progress VALUES (?, ?, 'pending', NULL, NULL, NULL)
      ", params = list(i, batch_file))
    } else if (!resume && existing$status == "completed") {
      # Skip completed batches if resume is TRUE
      next
    }
  }
  
  dbDisconnect(con)
  
  # Get batches to process
  con <- dbConnect(RSQLite::SQLite(), db_path)
  if (resume) {
    batches_to_process <- dbGetQuery(con, "
      SELECT batch_id, batch_file 
      FROM progress 
      WHERE status != 'completed'
      ORDER BY batch_id
    ")
  } else {
    batches_to_process <- dbGetQuery(con, "
      SELECT batch_id, batch_file 
      FROM progress 
      ORDER BY batch_id
    ")
  }
  dbDisconnect(con)
  
  if (nrow(batches_to_process) == 0) {
    cat("No batches to process!\n")
    return(invisible(NULL))
  }
  
  cat(sprintf("Processing %d batches with %d workers\n", 
              nrow(batches_to_process), n_workers))
  
  # Setup parallel processing
  plan(multisession, workers = n_workers)
  
  # Process batches in parallel
  results <- future_map_lgl(
    1:nrow(batches_to_process), 
    function(i) {
      batch_id <- batches_to_process$batch_id[i]
      batch_file <- batches_to_process$batch_file[i]
      
      cat(sprintf("Starting batch %d: %s\n", batch_id, basename(batch_file)))
      
      result <- process_batch_with_chunks(
        batch_file = batch_file,
        batch_id = batch_id,
        db_path = db_path,
        chunk_size = chunk_size
      )
      
      return(result)
    },
    .progress = TRUE,
    .options = furrr_options(seed = TRUE)
  )
  
  # Clean up
  plan(sequential)
  
  # Report results
  con <- dbConnect(RSQLite::SQLite(), db_path)
  on.exit(dbDisconnect(con))
  
  cat("\n=== PROCESSING SUMMARY ===\n")
  
  progress_summary <- dbGetQuery(con, "
    SELECT status, COUNT(*) as count 
    FROM progress 
    GROUP BY status
  ")
  print(progress_summary)
  
  cat("\n=== CHUNK SUMMARY ===\n")
  chunk_summary <- dbGetQuery(con, "
    SELECT 
      p.batch_file,
      COUNT(DISTINCT cp.chunk_id) as chunks_completed,
      COUNT(DISTINCT CASE WHEN cp.status = 'completed' THEN cp.chunk_id END) as chunks_successful
    FROM progress p
    LEFT JOIN chunk_progress cp ON p.batch_id = cp.batch_id
    GROUP BY p.batch_id, p.batch_file
    ORDER BY p.batch_id
  ")
  print(chunk_summary)
  
  cat("\n=== RESULTS SUMMARY ===\n")
  results_summary <- dbGetQuery(con, "
    SELECT 
      COUNT(*) as total_results,
      SUM(success) as successful_queries,
      COUNT(DISTINCT batch_id) as batches_with_results
    FROM results
  ")
  print(results_summary)
  
  # Check for failed batches
  failed_batches <- dbGetQuery(con, "
    SELECT batch_file, error_message 
    FROM progress 
    WHERE status = 'failed'
  ")
  
  if (nrow(failed_batches) > 0) {
    cat("\n=== FAILED BATCHES ===\n")
    print(failed_batches)
  }
  
  return(invisible(results))
}

# Utility function to check progress
check_progress <- function(db_path = "llm_results.db") {
  con <- dbConnect(RSQLite::SQLite(), db_path)
  on.exit(dbDisconnect(con))
  
  cat("=== BATCH PROGRESS ===\n")
  batch_progress <- dbGetQuery(con, "
    SELECT 
      batch_id,
      batch_file,
      status,
      start_time,
      end_time,
      CASE 
        WHEN end_time IS NOT NULL 
        THEN ROUND((julianday(end_time) - julianday(start_time)) * 24 * 60, 2)
        ELSE NULL
      END as duration_minutes
    FROM progress
    ORDER BY batch_id
  ")
  print(batch_progress)
  
  cat("\n=== OVERALL STATISTICS ===\n")
  stats <- dbGetQuery(con, "
    SELECT 
      COUNT(DISTINCT batch_id) as total_batches,
      COUNT(DISTINCT CASE WHEN status = 'completed' THEN batch_id END) as completed_batches,
      COUNT(*) as total_results,
      SUM(success) as successful_results,
      ROUND(100.0 * SUM(success) / COUNT(*), 2) as success_rate
    FROM results
  ")
  print(stats)
}

setwd("5_mapping_local_news/files")

# Get all batch files
batch_files <- list.files("subbatches", 
                          pattern = "^batch_\\d+\\.rds$",  # Matches one or more digits
                          full.names = TRUE)

# Extract batch numbers from filenames
batch_numbers <- as.numeric(gsub(".*batch_(\\d+)\\.rds$", "\\1", batch_files))

# Find indices of batches 127 to 204
indices_to_process <- which(batch_numbers >= 170 & batch_numbers <= 204)

time = Sys.time()

# Process all batches
process_batch_files_parallel(
  batch_files = batch_files[indices_to_process],
  db_path = "llm_results_170_204.db",
  n_workers = 6,  # Adjust based on your system
  chunk_size = 100,  # Adjust based on API performance
  resume = TRUE  # Will skip completed batches
)

Sys.time() - time
