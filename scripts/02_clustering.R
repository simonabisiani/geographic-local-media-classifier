# ===========================================================
# 1. Load Required Packages
# ===========================================================
library(tidyverse)
library(cluster)
library(dbscan)
library(factoextra)
library(clValid)
library(pheatmap)
library(corrplot)
library(RColorBrewer)
library(gridExtra)

metrics <- read_rds("data/features/metrics.rds")
metrics_simple <- metrics |> select(-num_articles, -num_locations, -moran_p, -mean_distance_km, -sd_distance, -max_distance_km, -mentions_per_lsoa, -geometry, -median_distance_km)
metrics_simple[] <- lapply(metrics_simple, unname)

colnames(metrics_simple) <- c(
  "Domain",
  "Area",
  "Radius",
  "Districts",
  "Entropy",
  "Gini",
  "MoranI",
  "DistCV",
  "Pct10km"
)

metrics_scales <- scale(metrics_simple |> select(-Domain))
# ===========================================================
# 1. Setup and Data Preparation
# ===========================================================
df <- metrics_simple 

prepare_datasets <- function(df) {
  features_full <- c("Area", "Radius", "Districts", "Entropy", "Gini", 
                     "MoranI", "DistCV", "Pct10km")
  features_minimal <- c("Area", "Districts", "DistCV", "MoranI")
  
  df_full_scaled <- scale(df[, features_full]) %>% as.data.frame()
  df_min_scaled <- scale(df[, features_minimal]) %>% as.data.frame()
  
  # PCA
  pca <- prcomp(df_full_scaled, scale. = FALSE)
  cum_var <- cumsum(pca$sdev^2 / sum(pca$sdev^2))
  
  pc_cutoffs <- c(80, 90, 95)
  pcs <- sapply(pc_cutoffs, function(cut) which(cum_var >= cut/100)[1])
  
  pca_datasets <- lapply(pcs, function(n) as.data.frame(pca$x[, 1:n]))
  names(pca_datasets) <- paste0("PCA_", pc_cutoffs)
  
  datasets <- list(Full = df_full_scaled,
                   Minimal = df_min_scaled) %>%
    c(pca_datasets)
  
  return(datasets)
}

datasets <- prepare_datasets(df)

# ===========================================================
# 2. Define Clustering Functions
# ===========================================================

get_silhouette <- function(labels, data) {
  if(length(unique(labels)) <= 1) return(NA)
  sil <- silhouette(labels, dist(data))
  mean(sil[,3])
}

cluster_kmeans <- function(data, k) {
  kmeans(data, centers = k, nstart = 100)$cluster
}

cluster_hclust <- function(data, k, method) {
  cutree(hclust(dist(data), method = method), k = k)
}

cluster_diana <- function(data, k) {
  cutree(diana(data), k = k)
}

cluster_hdbscan <- function(data, minPts) {
  hdbscan(data, minPts = minPts)$cluster
}

find_optimal_k <- function(data, method, linkage = NULL, k_range = 3:6) {
  sil_scores <- map_dbl(k_range, function(k) {
    clust <- switch(method,
                    kmeans = cluster_kmeans(data, k),
                    hierarchical = cluster_hclust(data, k, linkage),
                    diana = cluster_diana(data, k))
    get_silhouette(clust, data)
  })
  
  k_opt <- k_range[which.max(sil_scores)]
  list(k = k_opt, scores = sil_scores)
}

# ===========================================================
# 3. Run Clustering Experiments
# ===========================================================

results_list <- list()
silhouette_scores <- list()

set.seed(123)

for(name in names(datasets)) {
  data <- datasets[[name]]
  
  # K-means
  opt <- find_optimal_k(data, "kmeans")
  kmeans_labels <- cluster_kmeans(data, opt$k)
  silhouette_scores[[paste(name, "kmeans", sep = "_")]] <- get_silhouette(kmeans_labels, data)
  results_list[[paste(name, "kmeans", sep = "_")]] <- kmeans_labels
  
  # Hierarchical Ward
  opt <- find_optimal_k(data, "hierarchical", "ward.D2")
  hc_labels <- cluster_hclust(data, opt$k, "ward.D2")
  silhouette_scores[[paste(name, "hc_ward", sep = "_")]] <- get_silhouette(hc_labels, data)
  results_list[[paste(name, "hc_ward", sep = "_")]] <- hc_labels
  
  # Hierarchical Complete
  opt <- find_optimal_k(data, "hierarchical", "complete")
  hc_labels <- cluster_hclust(data, opt$k, "complete")
  silhouette_scores[[paste(name, "hc_complete", sep = "_")]] <- get_silhouette(hc_labels, data)
  results_list[[paste(name, "hc_complete", sep = "_")]] <- hc_labels
  
  # DIANA
  opt <- find_optimal_k(data, "diana")
  diana_labels <- cluster_diana(data, opt$k)
  silhouette_scores[[paste(name, "diana", sep = "_")]] <- get_silhouette(diana_labels, data)
  results_list[[paste(name, "diana", sep = "_")]] <- diana_labels
  
  # HDBSCAN
  minPts <- max(5, round(nrow(data)*0.01))
  hdb_labels <- cluster_hdbscan(data, minPts)
  
  sil_hdb <- ifelse(length(unique(hdb_labels[hdb_labels>0])) > 1,
                    get_silhouette(hdb_labels[hdb_labels>0], data[hdb_labels>0,]),
                    NA)
  
  silhouette_scores[[paste(name, "hdbscan", sep = "_")]] <- sil_hdb
  results_list[[paste(name, "hdbscan", sep = "_")]] <- hdb_labels
}

# ===========================================================
# 4. Compare Results
# ===========================================================

silhouette_df <- tibble(
  Approach = names(silhouette_scores),
  Silhouette = unlist(silhouette_scores)
) %>%
  separate(Approach, into = c("Dataset", "Method"), sep = "_", extra = "merge") %>%
  arrange(desc(Silhouette))

print(silhouette_df)

best <- silhouette_df %>% filter(Silhouette == max(Silhouette, na.rm=TRUE)) %>% slice(1)
best_label_col <- paste0(best$Dataset, "_", best$Method)

# ===========================================================
# 5. Agreement Analysis
# ===========================================================
library(mclust) # For ARI

cluster_mat <- do.call(cbind, results_list)
colnames(cluster_mat) <- names(results_list)
agreement <- outer(1:ncol(cluster_mat), 1:ncol(cluster_mat), Vectorize(function(i, j) {
  adjustedRandIndex(cluster_mat[,i], cluster_mat[,j])
}))

rownames(agreement) <- colnames(agreement) <- names(results_list)
pheatmap(agreement, main = "Adjusted Rand Index", display_numbers = TRUE)

# ===========================================================
# 6. Profile Best Clusters
# ===========================================================

# Create ordered list of methods by silhouette score (best first)
ordered_methods <- silhouette_df %>%
  filter(!is.na(Silhouette)) %>%
  arrange(desc(Silhouette)) %>%
  mutate(method_name = paste(Dataset, Method, sep = "_")) %>%
  pull(method_name)

# Assign clusters in order of performance
for(i in seq_along(ordered_methods)) {
  method_name <- ordered_methods[i]
  if(method_name %in% names(results_list)) {
    # Create column name with rank prefix
    col_name <- paste0(sprintf("%02d", i), "_", method_name)
    df[[col_name]] <- results_list[[method_name]]
  }
}

# Display the ranking
ranking_summary <- silhouette_df %>%
  filter(!is.na(Silhouette)) %>%
  arrange(desc(Silhouette)) %>%
  mutate(Rank = row_number()) %>%
  select(Rank, Dataset, Method, Silhouette)

googlesheets4::write_sheet(df,
                           ss = "https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1256911426#gid=1256911426",
                           sheet = "clustering_results")

write.csv(df, "data/clustering/outlet_cluster_directory.csv", row.names = FALSE)

# ===========================================================
# 7. Create Experiment Summary DataFrame with Cluster Profiles
# ===========================================================

# Define the original variables to summarize
original_vars <- c("Area", "Radius", "Districts", "Entropy", "Gini", 
                   "MoranI", "DistCV", "Pct10km")

# Function to calculate cluster statistics and profiles
get_cluster_profiles <- function(cluster_labels, data) {
  # Remove any outliers (cluster 0 from HDBSCAN)
  valid_indices <- cluster_labels > 0
  valid_clusters <- cluster_labels[valid_indices]
  valid_data <- data[valid_indices, ]
  
  if(length(valid_clusters) == 0) {
    return(list(
      n_clusters = 0,
      cluster_sizes = "No valid clusters",
      total_clustered = 0,
      outliers = sum(cluster_labels == 0),
      profiles = tibble()
    ))
  }
  
  # Calculate cluster profiles
  cluster_profiles <- valid_data %>%
    select(all_of(original_vars)) %>%
    mutate(cluster = valid_clusters) %>%
    group_by(cluster) %>%
    summarise(across(all_of(original_vars), ~mean(.x, na.rm = TRUE)), 
              n = n(), .groups = "drop") %>%
    arrange(cluster)
  
  cluster_table <- table(valid_clusters)
  
  list(
    n_clusters = length(unique(valid_clusters)),
    cluster_sizes = paste(as.numeric(cluster_table), collapse = ", "),
    total_clustered = length(valid_clusters),
    outliers = sum(cluster_labels == 0),
    profiles = cluster_profiles
  )
}

# Create summary dataframe
experiment_summary <- tibble()

for(method_name in names(results_list)) {
  cluster_labels <- results_list[[method_name]]
  
  # Get cluster statistics and profiles
  cluster_info <- get_cluster_profiles(cluster_labels, df)
  
  # Parse method name
  method_parts <- str_split(method_name, "_", n = 2)[[1]]
  dataset <- method_parts[1]
  method <- method_parts[2]
  
  # Get silhouette score
  sil_score <- silhouette_scores[[method_name]]
  
  # Create base row information
  base_info <- tibble(
    Experiment = method_name,
    Dataset = dataset,
    Method = method,
    Silhouette = round(sil_score, 4),
    N_Clusters = cluster_info$n_clusters,
    Cluster_Sizes = cluster_info$cluster_sizes,
    Total_Clustered = cluster_info$total_clustered,
    Outliers = cluster_info$outliers
  )
  
  # Add cluster-specific variable means
  if(nrow(cluster_info$profiles) > 0) {
    # Create columns for each cluster's variable means
    for(i in 1:nrow(cluster_info$profiles)) {
      cluster_num <- cluster_info$profiles$cluster[i]
      cluster_size <- cluster_info$profiles$n[i]
      
      # Add cluster size info
      base_info[[paste0(cluster_num, "_Size")]] <- cluster_size
      
      # Add variable means for this cluster
      for(var in original_vars) {
        col_name <- paste0(cluster_num, "_", var)
        base_info[[col_name]] <- round(cluster_info$profiles[[var]][i], 3)
      }
    }
  }
  
  experiment_summary <- bind_rows(experiment_summary, base_info)
}

# Sort by silhouette score (best first)
experiment_summary <- experiment_summary %>%
  arrange(desc(Silhouette))

# pivot longer for readability
experiment_summary_t <- experiment_summary %>%
  pivot_longer(
    cols = starts_with("0_") | starts_with("1_") | starts_with("2_") | starts_with("3_") | starts_with("4_") | starts_with("5_") | starts_with("6_"),
    names_to = c("Cluster", "Variable"),
    names_sep = "_",
    values_to = "Mean"
  ) %>%
  pivot_wider(
    names_from = Variable,
    values_from = Mean,
    values_fill = NA
  ) |> 
  # remove incomplete rows
  drop_na()

googlesheets4::write_sheet(experiment_summary_t,
                           ss = "https://docs.google.com/spreadsheets/d/1Y1xGVuFqMzaKnbQAQFgaIdgVJ1Ciik3UV5ougUzxXXE/edit?gid=1256911426#gid=1256911426",
                           sheet = "clusters_statistics_transposed")

# ===========================================================
# Clustering Analysis Visualizations
# ===========================================================

# Get the best clustering result
best_clusters <- df$`11_Minimal_kmeans`

# ===========================================================
# 1. PCA Scatter Plots
# ===========================================================

# PCA for visualization (using minimal feature set for consistency)
pca_viz <- prcomp(metrics_scales, scale. = FALSE)

var_explained_df <- data.frame(PC= paste0("PC",1:8),
                               var_explained=(pca_viz$sdev)^2/sum((pca_viz$sdev)^2)) |> 
  mutate(cum_var_explained=cumsum(var_explained))

a <- ggplot(data = var_explained_df, aes(x = PC)) +
  geom_step(
    aes(y = cum_var_explained * 100, group = 1),
    stat = "identity",
    # fill = "grey",
    # width = 0.9,
    alpha = 0.6
  ) +
  # Individual variance as connected points with line
  geom_line(
    aes(y = var_explained * 100), 
    color = "black",
    size = 0.5,
    group = 1
  ) +
  geom_text(
    aes(y = cum_var_explained * 100, 
        label = sprintf("%.1f", cum_var_explained * 100)),
    hjust = -0.8,
    vjust = -1,
    size = 3,
    color = "black"
  ) +
  geom_point(
    aes(y = var_explained * 100), 
    size = 3, 
    color = "black",
    shape = 19
  ) +
  # Labels on points (individual variance)
  geom_text(
    aes(y = var_explained * 100, 
        label = sprintf("%.1f", var_explained * 100)),
    vjust = -1.1,
    size = 3,
    color = "black"
  ) +
  scale_y_continuous(
    limits = c(0, 104),
    breaks = seq(0, 100, 50)
  ) +
  # Title and legend
  labs(
    title = "(a)",
    y = "Variance Explained (%)",
    x = ""
  ) +
  # Theme
  theme_minimal() +
  theme(
    # axis.text = element_text(size = 12, color = "black"),
    # axis.title = element_text(size = 14),
    plot.title = element_text(hjust = 0.5),
    legend.position = c(0.85, 0.85)
  ) 

# Extract PC1 loadings for ordering
pc1_order <- order(pca_viz$rotation[,1], decreasing = TRUE)
ordered_vars <- rownames(pca_viz$rotation)[pc1_order]
melt <- reshape2::melt(pca_viz$rotation[,1:6])
melt$Var1 <- factor(melt$Var1, levels = ordered_vars)
b <- ggplot(data = melt, aes(Var2, Var1, fill = value)) +
  labs(x = "", y = "", title = "(b)") +
  geom_tile(color = "white") +
  scale_fill_gradient2(low = "dark green", high = "purple", mid = "white", 
                       midpoint = 0.0, limit = c(-1,1), name="Factor loadings") +
  theme_minimal() + 
  theme(plot.title = element_text(hjust = 0.5),
        legend.title.position = "top",
        legend.position = "top")

g <- ggpubr::ggarrange(a, b, ncol = 2, nrow = 1, widths = c(1.5, 1))

ggplot2::ggsave(
  "img/figure2.pdf",
  g,
  width = 22.5,
  height = 7.5,
  dpi = 600,
  units = "cm"
)

# ===========================================================
# 2. Variable Scatter Plots (Key Relationships)
# ===========================================================

# Create the 4 most important scatter plots
p1 <- ggplot(df, aes(x = log10(Area), y = log10(Districts), color = as.factor(`11_Minimal_kmeans`))) +
  geom_point(size = 1.3, alpha = 0.5) +
  scale_color_brewer(type = "qual", palette = "Dark2", name = "Cluster") +
  labs(title = "Area vs Districts", 
       x = "log10(Area km²)", y = "log10(Districts)") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = "none")

p2 <- ggplot(df, aes(x = Entropy, y = DistCV, color = as.factor(`11_Minimal_kmeans`))) +
  geom_point(size = 1.3, alpha = 0.5) +
  scale_color_brewer(type = "qual", palette = "Dark2", name = "Cluster") +
  labs(title = "Diversity vs Distance Variability",
       x = "Entropy (bits)", y = "Distance CV") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = "none")

p3 <- ggplot(df, aes(x = log10(Area), y = Gini, color = as.factor(`11_Minimal_kmeans`))) +
  geom_point(size = 1.3, alpha = 0.5) +
  scale_color_brewer(type = "qual", palette = "Dark2", name = "Cluster") +
  labs(title = "Area vs Inequality",
       x = "log10(Area km²)", y = "Gini Coefficient") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = "none")

p4 <- ggplot(df, aes(x = Pct10km, y = MoranI, color = as.factor(`11_Minimal_kmeans`))) +
  geom_point(size = 1.3, alpha = 0.5) +
  scale_color_brewer(type = "qual", palette = "Dark2", name = "Cluster") +
  labs(title = "Proximity vs Spatial Clustering",
       x = "% within 10km", y = "Moran's I") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = "none")

# Create PCA biplot (larger, with legend)
p5 <- fviz_pca_biplot(pca_viz, 
                      col.ind = as.factor(best_clusters),
                      palette = "Dark2",
                      addEllipses = FALSE,
                      label = "var",
                      col.var = "black",
                      repel = TRUE,
                      legend.title = "Cluster",
                      title = "PCA Biplot") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = "bottom")

# Create 2x2 grid for the four scatter plots
top_grid <- ggpubr::ggarrange(p1, p2, p3, p4, 
                              ncol = 2, nrow = 2)

# Combine with biplot spanning full width
final_plot <- ggpubr::ggarrange(top_grid, p5, 
                                ncol = 2, nrow = 1,
                                widths = c(1, 1))

# Save with appropriate dimensions
ggplot2::ggsave(
  "img/figure5.pdf",
  final_plot,
  width = 30,
  height = 12,
  dpi = 600,
  units = "cm"
)