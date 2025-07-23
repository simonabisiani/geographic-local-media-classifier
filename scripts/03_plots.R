library(ggplot2)
library(tidyverse)
library(ggbeeswarm)
library(ggridges)
# ==============================================================================
# PLOT 1: Post-Geoparsing Descriptives
# ==============================================================================
sample <- read_csv("data/articles_sample.csv.zip")
sample_ <- sample |>
  select(unique_article_id, domain, year)

missing <- sample |>
  anti_join(
    data |> select(unique_article_id, domain, year),
    by = c("unique_article_id", "domain", "year")
  )

data <- read_rds("data/analysis_data.rds")
data_ <- data |> select(unique_article_id, domain, year) |> distinct()

nrow(data_) / nrow(sample)

p <- sample_ |>
  group_by(domain) |>
  summarise(`Pre-geoparsing` = n()) |>
  left_join(data_ |>
              group_by(domain) |>
              summarise(`Post-geoparsing` = n()),
            by = "domain") |>
  pivot_longer(
    cols = c("Pre-geoparsing", "Post-geoparsing"),
    names_to = "Dataset",
    values_to = "Number of Articles"
  ) |>
  ggplot(aes(x = `Number of Articles`, y = Dataset)) +
  geom_quasirandom(
    size = 0.6,
    alpha = 0.5,
    color = "grey",
    width = 0.4
  ) +
  geom_boxplot(width = 0.4,
               alpha = 0.5,
               outlier.shape = NA) +  # Add boxplot for median/IQR
  theme_minimal() +
  scale_x_log10(limits = c(10, 11000), position = "top") +
  labs(title = "(a)") +
  theme(
    plot.title = element_text(hjust = 0.5),
    axis.title.y = element_blank(),
    axis.text.y = element_text(size = 10)
  )

p1 <- sample_ |>
  group_by(domain) |>
  summarise(`Pre-geoparsing` = n()) |>
  left_join(data_ |>
              group_by(domain) |>
              summarise(`Post-geoparsing` = n()),
            by = "domain") |>
  mutate(`Share Geocoded` = `Post-geoparsing` / `Pre-geoparsing`) |>
  ggplot(aes(x = `Share Geocoded`)) +
  geom_histogram(
    fill = "grey",
    alpha = 0.9,
    color = "white",
    linewidth = 0.1
  ) +
  theme_minimal() +
  ylab("Number of Domains") +
  labs(title = "(b)") +
  theme(plot.title = element_text(hjust = 0.5))

p2 <- data |>
  group_by(domain) |>
  summarise(
    num_locations = n(),
    num_unique_locations = n_distinct(value),
    perc_unique = n_distinct(value) / n()
  ) |>
  ggplot(aes(x = num_locations, y = perc_unique)) +
  geom_jitter(alpha = 0.5, size = 0.6) +
  labs(title = "(c)", x = "Number of Locations", y = "Unique Locations (%)") +
  theme_minimal() +
  scale_x_log10(limits = c(20, 39000)) +
  theme(plot.title = element_text(hjust = 0.5))

g1 <-
  ggpubr::ggarrange(p,
                    ggpubr::ggarrange(p1, p2, ncol = 2, labels = c("", "")),
                    nrow = 2,
                    labels = "")


ggplot2::ggsave(
  "img/figure1.pdf",
  g1,
  width = 22.5,
  height = 9.25,
  dpi = 600,
  units = "cm"
)

# prompts <- read_csv("data/prompt_ingredients.csv")
# (prompts |> select(unique_article_id, article_text) |> n_distinct()) / nrow(sample)
# prompts |>
#   inner_join(read_rds("data/metrics.rds") |> select(domain), by = 'domain') |>
#   group_by(domain) |>
#   summarise(
#     `Number of Unique Locations` = n_distinct(unique_article_id),
#     `Number of Articles` = n_distinct(unique_article_id)
#   )
# select(value) |> n_distinct()

# ==============================================================================
# PLOT 2: Plot of features
# ==============================================================================
library(skimr)
metrics <- read_rds("data/features/metrics.rds")
skim(metrics)
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
metrics_long <- metrics_scales

metrics_long <- as_tibble(metrics_long) |>
  pivot_longer(cols = everything(),
               names_to = "metric",
               values_to = "value") |>
  mutate(metric = factor(metric, levels = names(metrics_simple)))

# ==============================================================================
# PLOT 3: Features
# ==============================================================================
# p <- GGally::ggpairs(metrics_scales, aes(alpha = 0.5),
#                      upper = list(continuous = GGally::wrap("cor", size = 2.5)))
# ggplot2::ggsave(
#   "correlations.pdf",
#   p,
#   width = 22.5,
#   height = 12.25,
#   dpi = 600,
#   units = "cm"
# )

# Create a mapping for metric names with units
metric_labels <- c(
  "Area" = "Area (km²)",
  "Radius" = "Radius (km)", 
  "Districts" = "Districts (n)",
  "Entropy" = "Entropy (bits)",
  "Gini" = "Gini (coeff)",
  "MoranI" = "Moran's I (index)",
  "DistCV" = "DistCV (coeff)",
  "Pct10km" = "Pct10km (%)"
)

a <- ggplot(metrics_simple |>
              select(-Domain) |> 
              pivot_longer(cols = everything(),
                           names_to = "metric",
                           values_to = "value") |>
              mutate(metric = factor(metric, levels = names(metrics_simple))), 
            aes(x = value)) +
  geom_histogram(color = "white", fill = "grey", alpha = 0.9, bins = 40) +
  facet_wrap(~ metric, scales = "free_x", ncol = 2,
             labeller = labeller(metric = metric_labels)) +
  scale_x_continuous(labels = function(x) {
    case_when(
      max(x, na.rm = TRUE) >= 1000 ~ paste0(round(x/1000, 1), "k"),
      max(x, na.rm = TRUE) >= 1 ~ as.character(round(x, 1)),
      TRUE ~ as.character(round(x, 3))
    )
  }) +
  theme_minimal() +
  theme(strip.text = element_text(size = 11),
        strip.background = element_blank(),
        plot.title = element_text(hjust = 0.5),
        axis.text.x = element_text(size = 9)) +
  labs(x = "", y = "Frequency", title = "(a)")

a <- ggplot(metrics_simple |>
              select(-Domain) |> 
              pivot_longer(cols = everything(),
                           names_to = "metric",
                           values_to = "value") |>
              mutate(metric = factor(metric, levels = names(metrics_simple))), 
            aes(x = value, y = "")) +  # Use empty string for x to create single category per facet
  geom_quasirandom(
    size = 1,
    alpha = 0.2,
    color = "grey",
    width = 0.3
  ) +
  geom_boxplot(width = 0.4,
               alpha = 0.8,
               outlier.shape = NA) +
  facet_wrap(~ metric, scales = "free_x", ncol = 2,
             labeller = labeller(metric = metric_labels)) +
  scale_x_continuous(labels = function(x) {
    case_when(
      max(x, na.rm = TRUE) >= 1000 ~ paste0(round(x/1000, 1), "k"),
      max(x, na.rm = TRUE) >= 1 ~ as.character(round(x, 1)),
      TRUE ~ as.character(round(x, 3))
    )
  }) +
  theme_minimal() +
  theme(strip.text = element_text(size = 10, hjust = 0),
        strip.background = element_blank(),
        plot.title = element_text(hjust = 0.5),
        panel.grid.major = element_blank())+
  labs(x = "", y = "", title = "(a)") 

b <- GGally::ggcorr(metrics_scales, method = c("everything", "pearson"), label = TRUE,
                    low = "dark green", mid = "white", high = "purple", label_size = 3, size = 3.5) + 
  ggtitle("(b)")+
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = "bottom",
        axis.text = element_text(size = 10)) 

g2 <- ggpubr::ggarrange(a,
                        b,
                        widths = c(1, 1),
                        ncol = 2)

ggplot2::ggsave(
  "img/figure3",
  g2,
  width = 30,
  height = 12,
  dpi = 600,
  units = "cm"
)

# Administrative Reach
supergroup_distribution <- readr::read_rds("data/features/demographic_reach.rds")

b <- supergroup_distribution |>
  pivot_longer(cols = -domain,
               names_to = "metric",
               values_to = "value") |>
  mutate(
    metric = case_when(
      metric == "Suburbanites and Peri-Urbanites" ~ "Suburbanites\nand Peri-Urbanites",
      metric == "Retired Professionals" ~ "Retired\nProfessionals",
      metric == "Baseline UK" ~ "Baseline UK",
      metric == "Multicultural and Educated Urbanites" ~ "Multicultural and\nEducated Urbanites",
      metric == "Semi- and Un-Skilled Workforce" ~ "Semi- and\nUn-Skilled Workforce",
      metric == "Ethnically Diverse Suburb-an Professionals" ~ "Ethnically Diverse\nSuburb-an Professionals",
      metric == "Low-Skilled Migrant and Student Communities" ~ "Low-Skilled Migrant\nand Student Communities",
      metric == "Legacy Communities" ~ "Legacy\nCommunities",
      TRUE ~ metric
    )
  ) |>
  ggplot(aes(x = value, y = reorder(metric, value, FUN = median))) +
  geom_density_ridges(
    fill = "grey",
    color = 'grey50',
    alpha = 0.6,
    scale = 2,
    linewidth = 0.2,
    end = "cut"
  ) +
  labs(x = "Percentage of Mentions", title = "(a)") +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 9, color = "black"),
    axis.title.y = element_blank(),
    plot.title = element_text(hjust = 0.5)
  ) +
  scale_x_continuous(limits = c(-10, 60))

# Distance Decay
outlets_with_distances <- read_rds("data/features/outlets_with_distances.rds")
e <- outlets_with_distances %>%
  group_by(domain) %>%
  arrange(distance_from_outlet) %>%
  mutate(cumulative_pct = row_number() / n()) %>%
  ungroup() %>%
  filter(between(
    distance_from_outlet,
    quantile(distance_from_outlet, 0.00),
    quantile(distance_from_outlet, 0.95)
  )) |>
  ggplot(aes(x = distance_from_outlet, y = cumulative_pct, group = domain)) +
  geom_line(linewidth = 0.3, alpha = 0.1) +
  labs(x = "Distance from Outlet (km)", y = "Cumulative Percentage", title = "(b)") +
  theme_minimal() +
  theme(legend.position = "none",
        plot.title = element_text(hjust = 0.5)) +
  scale_x_sqrt()

g2 <- ggpubr::ggarrange(
  b,
  e,
  ncol = 2,
  heights = c(1.5, 1))

ggplot2::ggsave(
  "img/figure4.pdf",
  g2,
  width = 25,
  height = 10,
  dpi = 600,
  units = "cm")
