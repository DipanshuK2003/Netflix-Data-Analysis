# Netflix Data Analysis | MovieLens 25M Dataset

[![Tableau Dashboard](https://img.shields.io/badge/Tableau-Dashboard-blue)](https://public.tableau.com/app/profile/dipanshu.kumar1559/viz/NetflixDataAnalysisMovieLens25Mdataset/Dashboard)
[![Python](https://img.shields.io/badge/Python-3.8%2B-brightgreen)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A comprehensive end-to-end data analytics project analyzing 58,675 movies and 25 million ratings from the MovieLens 25M dataset. This project combines statistical analysis, data visualization, and business intelligence to uncover insights about movie quality, popularity, and content characteristics.

---

## Live Dashboard

**[View Interactive Dashboard →](https://public.tableau.com/app/profile/dipanshu.kumar1559/viz/NetflixDataAnalysisMovieLens25Mdataset/Dashboard)**

![Dashboard Preview](assets/Netflix%20%7C%20Summary.png)

---

## Project Overview

This project answers critical questions about the relationship between movie popularity and quality through rigorous statistical analysis:

- Does popularity predict quality?
- Which genres perform best?
- How do curated genome tags and user tags influence ratings?
- What patterns emerge from community engagement?

**Key Findings:**
- Weak correlation (r = 0.074) between popularity and quality—volume ≠ excellence
- Significant genre effects on ratings (Kruskal-Wallis H = 1111.59, p < 0.001)
- User-tagged movies rate 0.31 points higher (Cohen's d = 0.63)
- Genome tags are strong quality predictors (H = 2812.79, p < 0.001)

---

## Project Pipeline

![Project Flowchart](assets/Flowchart.png)

---

## Repository Structure

```

│
├── assets/                           # Visual assets for dashboard and documentation
│ ├── Design Template.png             # Dashboard design mockup
│ ├── Flowchart.png                   # Project workflow diagram
│ ├── Netflix | Summary.png           # Dashboard screenshot
│ └── netflix_logo.png                # Logo asset
│
├── database exploration/             # SQL exploration and custom queries
│ ├── Netflix_Movie_Analysis.sql      # Database exploration queries
│ ├── top_titles_by_decade.csv        # Decade-based rankings for Tableau
│ └── top_titles_by_genre.csv         # Genre-based rankings for Tableau
│
├── Netflix Data Analysis.ipynb       # Main analysis notebook (11 questions)
├── netflix_ingestion.py              # Raw data ingestion script
├── get_movie_summary.py              # Summary table creation & data cleaning
├── Netflix Dashboard.twbx            # Tableau packaged workbook
├── NETFLIX DATA ANALYSIS.pdf         # Full project report
├── requirements.txt                  # Python dependencies
├── LICENSE                           # MIT License
└── README.md                         # This file

```


---

## Getting Started

### Prerequisites

- **Python 3.8+**
- **PostgreSQL 12+**
- **Tableau Desktop/Public** (for dashboard)
- **Jupyter Notebook/Lab**

### Installation

1. **Clone the repository:**

```

git clone https://github.com/DipanshuK2003/Netflix-Data-Analysis.git
cd Netflix-Data-Analysis

```

2. **Install Python dependencies:**

```

pip install -r requirements.txt

```

3. **Download the dataset:**
- Download MovieLens 25M from [GroupLens](https://grouplens.org/datasets/movielens/25m/)
- Extract files to a `data/` folder (create if needed)

4. **Set up PostgreSQL database:**

```

Create database
createdb movielens

Set environment variables (optional)
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=movielens
export DB_USER=your_username
export DB_PASSWORD=your_password

```


---

## Data Pipeline

### Step 1: Raw Data Ingestion

```

python netflix_ingestion.py

```

- Loads CSV files into PostgreSQL
- Creates tables: `movies`, `ratings`, `tags`, `genome_tags`, `genome_scores`
- Handles data types and indexes

### Step 2: Summary Table Creation

```

python get_movie_summary.py

```

- Aggregates ratings per movie
- Computes top genome tags (relevance > 0.5)
- Identifies most common user tags
- Generates `summary_table` for analysis

### Step 3: Analysis
Open `Netflix Data Analysis.ipynb` in Jupyter:

```

jupyter notebook "Netflix Data Analysis.ipynb"

```


### Step 4: Visualization
Open `Netflix Dashboard.twbx` in Tableau Desktop or view the [live dashboard](https://public.tableau.com/app/profile/dipanshu.kumar1559/viz/NetflixDataAnalysisMovieLens25Mdataset/Dashboard).

---

## Analysis Phases

### Phase 1: Basic EDA
- Dataset overview (58,675 movies, 25M ratings)
- Distribution analysis (ratings, genres, popularity)

### Phase 2: Outlier Analysis
- Identifying extreme values in ratings and popularity
- Statistical thresholds using IQR and z-scores

### Phase 3: Popularity vs Quality
- Pearson correlation with 95% confidence intervals
- Popularity tier comparisons (Top 10% vs Bottom 90%)

### Phase 4: Statistical Testing
- Kruskal-Wallis H-test for genre differences
- Mann-Whitney U tests for tier comparisons
- Multiple testing correction (Benjamini-Hochberg FDR)

### Phase 5: Genome Tag Analysis
- 1,127 curated tags, 23.5% coverage
- Tag frequency and rating correlation
- Statistical significance testing

### Phase 6: User Tag Analysis
- 73,050 unique user tags, 71% coverage
- Community engagement patterns
- Tagged vs non-tagged movie comparison

---

## Dashboard Features

**Interactive Elements:**
- Genre filters
- Rating range sliders
- Decade selection
- Dynamic top-N parameters

**Visualizations:**
- KPI cards (Total Movies, Ratings, Genres, Coverage)
- Popularity vs Quality scatter plot
- Genre performance comparison
- Tag frequency rankings
- Quadrant analysis (Blockbusters, Hidden Gems, Overhyped, Obscure)
- Top titles by decade
- Content trait analysis (genome relevance vs ratings)

---

## Key Insights

1. **Popularity ≠ Quality**: Weak correlation (r = 0.074) dispels the myth that popular movies are always better
2. **Genre Matters**: Significant rating differences across genres (War, Drama outperform Horror, Thriller)
3. **Community Signal**: User tagging correlates with higher ratings—engaged audiences mark quality content
4. **Tag Power**: Genome tags are strong quality predictors, ideal for content-based recommendations
5. **Hidden Gems**: Many high-quality films remain underexposed—opportunities for targeted promotion

---

## Technologies Used

| Category | Tools |
|----------|-------|
| **Languages** | Python, SQL |
| **Database** | PostgreSQL |
| **Analysis** | Pandas, NumPy, SciPy, Statsmodels |
| **Visualization** | Matplotlib, Seaborn, Tableau |
| **Environment** | Jupyter Notebook, Anaconda |
| **Version Control** | Git, GitHub |

---

## Project Report

Full analysis report with methodology, statistical tests, and business recommendations: **[NETFLIX DATA ANALYSIS.pdf](NETFLIX%20DATA%20ANALYSIS.pdf)**

---

## Contact

**Dipanshu Kumar**
- Email: kayhiusy@gmail.com
- GitHub: [@DipanshuK2003](https://github.com/DipanshuK2003)
- Tableau Public: [Profile](https://public.tableau.com/app/profile/dipanshu.kumar1559)
- LinkedIn: [linkedin.com/in/Dipanshu Kumar](https://www.linkedin.com/in/dipanshu-kumar-61a21322a/)  

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **GroupLens Research** for the MovieLens 25M dataset
- **MovieLens** for providing high-quality movie rating data
- Inspiration from data-driven content recommendation systems

---

## References

- [MovieLens 25M Dataset](https://grouplens.org/datasets/movielens/25m/)
- [GroupLens Research](https://grouplens.org/)
- Statistical methods: Kruskal-Wallis H-test, Mann-Whitney U test, Pearson correlation
- Multiple testing correction: Benjamini-Hochberg FDR

---

**⭐ If you found this project helpful, please consider giving it a star!**

