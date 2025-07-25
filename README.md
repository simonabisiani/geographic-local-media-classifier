# Mapping News Geography: A Computational Framework for Classifying Local Media Through Geographic Coverage Patterns

This repository contains the code, results data, and supplementary materials for the paper titled *"Mapping News Geography: A Computational Framework for Classifying Local Media Through Geographic Coverage Patterns"*. The study introduces a computational framework to classify local news outlets based on their geographic coverage patterns using geoparsing, feature engineering, and clustering analysis.

## Repository Structure

The repository is organised as follows (here detailed only at the folder level, with the addition of key files):
```bash
├── data
│   ├── clustering
│   ├── features
├── img
└── scripts
    ├── 01_feature_engineering.R
    ├── 02_clustering.R
    ├── 03_plots.R
    ├── 04_morans_sensitivity_analyses.R
    └── data_prep
```

- `scripts/`: R scripts for geoparsing, feature engineering, clustering, and plotting.
  - `data_prep/`: These are scripts related to geoparsing. We used a combination of HPC and local machines to run the LLM toponym resolution step, thus we provide a variety of scripts suited to these approaches for geocoding locations. Some of the data files loaded inside of these scripts are not provided as part of this repo due to their size. As such, please get in touch if you would like to reproduce the geoparsing steps.
- `data/features`: Intermediate and final files related to the features developed.
- `data/clustering`: Clustering assignment results from the full set of experiments.
- `img/`: Figures generated by the analysis (PDF format).
- `manuscript/`: Preprint file.

## Replication Data
The necessary files to replicate this study reside at the following permanent storage repository: https://doi.org/10.7910/DVN/T7SE5F
To use them in combination with the scripts provided here and the file path structures in the code, download the following files and place them in the `data` folder.
- `articles_sample.csv.zip`: this is the article sample extracted from UKTWitNewsCor.
- `articles_small.rds`: this is the sample geocoded, where each row is a location mentioned by an outlet and a variable `n` tracks the frequency of this location mentions for this given outlet.
- `articles_data.rds`: this is the long format of the file above.
- `combined_boundaries.rds`: this is a spatial file with census district level boundaries for England, Wales, Scotland, and Northern Ireland.


Due to availability upon application only, we cannot share the LSOA classification data, and we urge you to apply for access here: https://data.geods.ac.uk/dataset/lsoac. These files are in the scripts loaded from a folder, not provided here, called 'lsoa_lad_data'.

## Key Features of the Study

1. **Geoparsing Pipeline**: Uses large language models (LLMs) for toponym disambiguation to extract geographic locations from news articles.
2. **Spatial Metrics**: Develops eight spatial metrics across four dimensions: spatial extent, administrative reach, spatial heterogeneity, and distance decay.
3. **Clustering Analysis**: Identifies six distinct outlet types (e.g., hyperlocal, local-regional, national) through unsupervised clustering on these metrics.
4. **Scalable Framework**: Provides a modular, open-source approach for mapping local news coverage, applicable to other datasets and media systems.

## How to Use This Repository

1. **Reproduce the Analysis**:
   - Run the scripts in the `scripts` folder in numerical order to replicate the analysis.
   - The `data` folder contains initial, intermediate, and final datasets used in the analysis.

2. **Explore the Results**:
   - The `clustering` folder contains the final outlet classifications.
   - Figures in the `img` folder visualise key findings from the study.

3. **Extend the Framework**:
   - The modular design allows adaptation to other geographic contexts or datasets.
   - Feature engineering and clustering scripts can be modified for different research questions.

## Dependencies

- R (version 4.4.1 or later)
- Key R packages: `tidyverse`, `sf`, `spdep`, `concaveman`, `ineq`, `geosphere`, `DescTools`

## Citation

If you use this framework or findings in your work, please cite the accompanying paper:

@article{bisiani_2025_, title={Mapping News Geography: A Computational Framework for Classifying Local Media Through Geographic Coverage Patterns}, author={Bisiani, S., Gulyas, A., and Bahareh Heravi}, year={2025}, journal={Forthcoming}}

## Contact

For questions or collaboration, contact [S. Bisiani](mailto:s.bisiani@surrey.ac.uk).