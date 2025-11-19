"""
MOVIELENS SUMMARY TABLE GENERATION - DATA AGGREGATION & CLEANING
=================================================================

PROJECT: Netflix / MovieLens Data Analytics
AUTHOR: Dipanshu Kumar
DATE: 26/10/2025

PURPOSE:
- Create a comprehensive movie summary table from raw MovieLens 25M database tables
- Implement improved year extraction with regex pattern matching
- Validate data quality: check for duplicate genres during explosion
- Clean data: remove movies with NULL years or NULL ratings
- Automate summary table generation for reproducible downstream analysis

DATA SOURCE ANALYSIS:
• `movies` contains movie titles, genres (pipe-delimited), and movieId
• `ratings` contains all user ratings per movie
• `tags` contains user-supplied tags per movie
• `genome_scores` and `genome_tags` provide automated tag relevance scores

BUSINESS NEED FOR SUMMARY TABLE:
As the data required for analysis is distributed across different tables, we need to create 
a unified summary table containing:
• Movie title and year (extracted via regex from title)
• Genre categorization (exploded from pipe-delimited list)
• Average rating and rating count
• Most relevant genome tag with relevance score
• Most common user-supplied tag

DATA QUALITY RULES:
• Remove movies without extractable year information
• Remove movies without any ratings (NULL avg_rating)
• Validate no duplicate genres in source data

PERFORMANCE OPTIMIZATION BENEFITS:
• Expensive joins, aggregations, and genre explosions are pre-computed
• Cleaned data ensures downstream analyses work with complete records only

DATA FLOW:
Raw Tables → SQL Summary Query → Quality Validation → Data Cleaning → Summary Table
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# ==================== LOGGING SETUP ====================
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/get_movie_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

# ==================== DATABASE CONFIGURATION ====================
# Database configuration - REPLACE WITH YOUR CREDENTIALS
DB_USER = "your_postgres_username"
DB_PASS = "your_postgres_password" 
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "vendor_db"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,  # Verify connections before use
)

# ==================== CORE BUSINESS LOGIC - SQL QUERIES ====================

# Main summary table creation query
MOVIE_SUMMARY_SQL = """
-- UNIFIED MOVIE SUMMARY QUERY
-- Combines movies, ratings, tags, and genome data
-- Uses improved regex-based year extraction

WITH movie_base AS (
    -- Extract year from title using regex pattern matching
    -- Handles edge cases where year format varies or is missing
    -- Each movie-genre combination becomes a separate row
    SELECT 
        m."movieId",
        m.title,
        CAST(
            CASE 
                WHEN m.title ~ '.*\\((\\d{4})\\)' 
                THEN (REGEXP_MATCH(m.title, '.*\\((\\d{4})\\)'))[1]
                ELSE NULL
            END AS INTEGER
        ) AS year,
        genre,
        ROUND(AVG(r.rating)::numeric, 2) AS avg_rating,
        COUNT(r.rating) AS rating_count
    FROM movies m
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(m.genres, '|')) AS genre
    LEFT JOIN ratings r ON m."movieId" = r."movieId"
    GROUP BY m."movieId", m.title, genre
),
top_genome AS (
    -- Get the most relevant genome tag for each movie
    -- Genome tags are system-generated descriptive tags with relevance scores
    SELECT 
        gs."movieId",
        gt.tag AS top_genome_tag,
        gs.relevance AS genome_relevance
    FROM (
        SELECT 
            "movieId",
            "tagId",
            relevance,
            ROW_NUMBER() OVER (PARTITION BY "movieId" ORDER BY relevance DESC) as rn
        FROM genome_scores
    ) gs
    JOIN genome_tags gt ON gs."tagId" = gt."tagId"
    WHERE gs.rn = 1
),
common_user_tag AS (
    -- Get the most common user-supplied tag for each movie
    -- User tags are free-form text tags created by users
    SELECT 
        t."movieId",
        t.tag AS most_common_user_tag
    FROM (
        SELECT 
            "movieId",
            tag,
            COUNT(*) as tag_count,
            ROW_NUMBER() OVER (PARTITION BY "movieId" ORDER BY COUNT(*) DESC, tag) as rank
        FROM tags
        GROUP BY "movieId", tag
    ) t
    WHERE t.rank = 1
)
-- Final summary: join all data sources
SELECT 
    mb."movieId",
    mb.title,
    mb.year,
    mb.genre,
    mb.avg_rating,
    mb.rating_count,
    tg.top_genome_tag,
    tg.genome_relevance,
    cut.most_common_user_tag
FROM movie_base mb
LEFT JOIN top_genome tg ON mb."movieId" = tg."movieId"
LEFT JOIN common_user_tag cut ON mb."movieId" = cut."movieId"
ORDER BY mb."movieId", mb.genre
"""

# Data quality validation query - check for duplicate genres
GENRE_DUPLICATE_CHECK_SQL = """
-- DATA QUALITY CHECK: Detect duplicate genres in pipe-delimited genre strings
-- This validation ensures genre explosion produces correct distinct genres

WITH genre_analysis AS (
    SELECT 
        "movieId",
        title,
        genres,
        STRING_TO_ARRAY(genres, '|') as genre_array,
        ARRAY_LENGTH(STRING_TO_ARRAY(genres, '|'), 1) as original_count,
        ARRAY_LENGTH(ARRAY(SELECT DISTINCT UNNEST(STRING_TO_ARRAY(genres, '|'))), 1) as unique_count
    FROM movies
    WHERE ARRAY_LENGTH(STRING_TO_ARRAY(genres, '|'), 1) != 
          ARRAY_LENGTH(ARRAY(SELECT DISTINCT UNNEST(STRING_TO_ARRAY(genres, '|'))), 1)
)
SELECT COUNT(*) as movies_with_duplicate_genres
FROM genre_analysis
"""

# Data cleaning queries
CLEAN_NULL_YEARS_SQL = """
-- Remove movies with NULL year (unable to extract year from title)
-- Ensures all movies in summary have valid year information
DELETE FROM summary_table 
WHERE "movieId" IN (
    SELECT DISTINCT "movieId" 
    FROM summary_table 
    WHERE year IS NULL
)
"""

CLEAN_NULL_RATINGS_SQL = """
-- Remove movies with NULL avg_rating (no ratings data available)
-- Ensures all movies in summary have rating information for analysis
DELETE FROM summary_table 
WHERE "movieId" IN (
    SELECT DISTINCT "movieId" 
    FROM summary_table 
    WHERE avg_rating IS NULL
)
"""

# ==================== CORE FUNCTIONS ====================

def validate_genre_quality(engine):
    """
    Data quality check: Verify no duplicate genres exist in pipe-delimited strings.
    
    BUSINESS LOGIC:
    - Checks if any movie has duplicate genres in its genre string (e.g., "Action|Drama|Action")
    - Logs quality metrics for monitoring
    - Raises warning if duplicates detected (suggests upstream data issue)
    
    RETURNS:
        int: Count of movies with duplicate genres
    """
    logging.info("Running data quality check: validating genre uniqueness…")
    
    with engine.connect() as conn:
        result = conn.execute(text(GENRE_DUPLICATE_CHECK_SQL))
        row = result.fetchone()
        duplicate_count = row[0] if row else 0
        
        if duplicate_count > 0:
            logging.warning(f"⚠️ Data Quality Issue: {duplicate_count} movies have duplicate genres")
            print(f"⚠️ Data Quality Warning: {duplicate_count} movies have duplicate genres in source data")
        else:
            logging.info("✅ Genre quality check passed: No duplicate genres detected")
            print("✅ Genre quality validation passed")
        
        return duplicate_count

def create_movie_summary(engine):
    """
    Execute the SQL query to create the summary table.
    
    BUSINESS LOGIC:
    - Runs comprehensive SQL aggregation across all source tables
    - Explodes genres into separate rows (one row per movie-genre combination)
    - Uses improved regex-based year extraction
    - Calculates average ratings and counts per movie-genre
    - Joins top genome tags and most common user tags
    
    PERFORMANCE NOTE:
    This query performs expensive joins and aggregations. Pre-computing avoids
    repeated execution during analysis and dashboarding.
    
    RETURNS:
        int: Initial row count before cleaning
    """
    logging.info("Creating movie summary table with improved year extraction…")
    
    with engine.connect() as conn:
        # Drop existing summary table if it exists
        conn.execute(text("DROP TABLE IF EXISTS summary_table"))
        conn.commit()
        
        # Create new summary table
        create_sql = f"CREATE TABLE summary_table AS {MOVIE_SUMMARY_SQL}"
        conn.execute(text(create_sql))
        conn.commit()
        
        # Get initial row count
        result = conn.execute(text("SELECT COUNT(*) FROM summary_table"))
        row_count = result.fetchone()[0]
        
    logging.info(f"✅ Summary table created with {row_count:,} initial rows")
    print(f"✅ Summary table created: {row_count:,} initial movie-genre combinations")
    return row_count

def clean_null_years(engine):
    """
    Remove movies where year could not be extracted from title.
    
    BUSINESS LOGIC:
    - Removes all genre rows for movies without valid year information
    - Ensures downstream temporal analysis has complete data
    
    RETURNS:
        int: Number of rows deleted
    """
    logging.info("Cleaning data: removing movies with NULL years…")
    
    with engine.connect() as conn:
        # Get count before deletion
        result = conn.execute(text("SELECT COUNT(*) FROM summary_table WHERE year IS NULL"))
        null_count = result.fetchone()[0]
        
        # Execute deletion
        conn.execute(text(CLEAN_NULL_YEARS_SQL))
        conn.commit()
        
    logging.info(f"✅ Removed {null_count:,} rows with NULL years")
    print(f"✅ Removed {null_count:,} rows with NULL years")
    return null_count

def clean_null_ratings(engine):
    """
    Remove movies where no ratings exist (avg_rating is NULL).
    
    BUSINESS LOGIC:
    - Removes all genre rows for movies without any rating data
    - Ensures downstream rating analysis has complete data
    
    RETURNS:
        int: Number of rows deleted
    """
    logging.info("Cleaning data: removing movies with NULL avg_rating…")
    
    with engine.connect() as conn:
        # Get count before deletion
        result = conn.execute(text("SELECT COUNT(*) FROM summary_table WHERE avg_rating IS NULL"))
        null_count = result.fetchone()[0]
        
        # Execute deletion
        conn.execute(text(CLEAN_NULL_RATINGS_SQL))
        conn.commit()
        
    logging.info(f"✅ Removed {null_count:,} rows with NULL avg_rating")
    print(f"✅ Removed {null_count:,} rows with NULL avg_rating")
    return null_count

def display_summary_stats(engine):
    """
    Display final summary statistics about the cleaned table.
    
    Provides visibility into:
    - Total rows after cleaning
    - Remaining NULL counts per column
    - Sample data preview
    """
    logging.info("Generating final summary statistics…")
    
    with engine.connect() as conn:
        # Row count
        result = conn.execute(text("SELECT COUNT(*) FROM summary_table"))
        total_rows = result.fetchone()[0]
        
        # NULL counts per column (should be 0 for year and avg_rating after cleaning)
        null_check_sql = """
        SELECT 
            COUNT(*) FILTER (WHERE year IS NULL) as null_years,
            COUNT(*) FILTER (WHERE avg_rating IS NULL) as null_avg_ratings,
            COUNT(*) FILTER (WHERE top_genome_tag IS NULL) as null_genome_tags,
            COUNT(*) FILTER (WHERE most_common_user_tag IS NULL) as null_user_tags
        FROM summary_table
        """
        result = conn.execute(text(null_check_sql))
        null_stats = result.fetchone()
        
        # Sample data
        sample_df = pd.read_sql("SELECT * FROM summary_table LIMIT 5", con=engine)
    
    print(f"\n📊 Final Summary Table Statistics:")
    print(f"   • Total rows (after cleaning): {total_rows:,}")
    print(f"   • NULL years: {null_stats[0]:,}")
    print(f"   • NULL avg_ratings: {null_stats[1]:,}")
    print(f"   • NULL genome_tags: {null_stats[2]:,}")
    print(f"   • NULL user_tags: {null_stats[3]:,}")
    print(f"\n📋 Sample data (first 5 rows):")
    print(sample_df.to_string(index=False))
    
    logging.info(f"Final stats - Total: {total_rows:,}, NULL years: {null_stats[0]:,}, NULL ratings: {null_stats[1]:,}")

def main():
    """
    Main execution for MovieLens summary pipeline.
    
    WORKFLOW:
    1. Validate data quality: Check for duplicate genres
    2. Create summary table with improved year extraction
    3. Clean data: Remove movies with NULL years
    4. Clean data: Remove movies with NULL ratings
    5. Display final summary statistics
    
    DATA QUALITY RULES:
    - Movies without extractable years are removed
    - Movies without ratings are removed
    - Only complete records remain for analysis
    """
    logging.info("===== Building Movie Summary Table =====")
    print("🎬 MovieLens Summary Table Generation Starting...")
    
    try:
        # Step 1: Data Quality Validation
        print("\n🔍 Validating data quality...")
        duplicate_count = validate_genre_quality(engine)
        
        # Step 2: Create Summary Table
        print("\n📊 Creating summary table...")
        initial_count = create_movie_summary(engine)
        
        # Step 3: Clean NULL Years
        print("\n🧹 Cleaning data: removing NULL years...")
        removed_years = clean_null_years(engine)
        
        # Step 4: Clean NULL Ratings
        print("\n🧹 Cleaning data: removing NULL ratings...")
        removed_ratings = clean_null_ratings(engine)
        
        # Step 5: Display Final Statistics
        display_summary_stats(engine)
        
        # Summary
        total_removed = removed_years + removed_ratings
        final_count = initial_count - total_removed
        
        print("\n🎉 Summary table generation complete!")
        print(f"📌 Initial rows: {initial_count:,}")
        print(f"📌 Removed rows: {total_removed:,} (years: {removed_years:,}, ratings: {removed_ratings:,})")
        print(f"📌 Final rows: {final_count:,}")
        print(f"📌 Table name: summary_table")
        
        logging.info("✅ Movie summary pipeline completed successfully")
        logging.info(f"Summary: {initial_count:,} → {final_count:,} rows (removed {total_removed:,})")
        
    except Exception as e:
        logging.exception(f"❌ Pipeline failed: {e}")
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    """
    Script entry point for direct execution.

    USAGE:
    python get_movie_summary.py

    BUSINESS VALUE:
    Creates an optimized, cleaned summary table for movie-level analytics.
    Ensures data quality by removing incomplete records while preserving
    performance benefits of pre-aggregated data.
    """
    main()
