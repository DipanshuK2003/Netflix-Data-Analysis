/* =======================================================
   NETFLIX DATA ANALYSIS - COMPLETE DATA PIPELINE
   =======================================================

   Project: Netflis Data Analysis
   Author: Dipanshu Kumar
   Dataset: MovieLens 25M dataset having 25 million ratings, 62,000 movies, 162,000 users
   
   OBJECTIVES:
   1. Understand dataset structure and relationships
   2. Perform comprehensive data quality assessment
   3. Create optimized analytical summary table
   4. Prepare data for Tableau dashboard and insights
   =======================================================  
*/



/* =======================================================
   SECTION 1: DATABASE DISCOVERY & INITIAL ASSESSMENT
   ======================================================= 
   Purpose: Understand available data sources and their structure
   Key Findings: 6 tables covering movies, ratings, tags, and genome data
*/

-- List all tables in the database
SELECT table_name AS "name"
FROM information_schema.tables
WHERE table_schema = 'public';

-- Preview each table structure and sample data
SELECT * FROM movies LIMIT 10;           -- Movie metadata with genres and titles
SELECT * FROM genome_scores LIMIT 10;    -- Algorithmic movie tag relevance scores
SELECT * FROM genome_tags LIMIT 30;      -- Tag definitions for genome scoring
SELECT * FROM links LIMIT 10;            -- External database identifiers (IMDb, TMDb)
SELECT * FROM ratings LIMIT 10;          -- 25M user ratings (core engagement data)
SELECT * FROM tags LIMIT 10;             -- User-generated movie tags



/* =======================================================
   SECTION 2: DATASET VOLUME ANALYSIS
   ======================================================= 
   Purpose: Understand data scale and distribution
   Key Insights: 25M ratings, 62K movies, robust dataset for analysis
*/

-- Comprehensive row counts across all tables
SELECT COUNT(*) FROM movies;          -- 62,423 movies
SELECT COUNT(*) FROM genome_scores;   -- 15,584,448 genome score records
SELECT COUNT(*) FROM genome_tags;     -- 1,128 unique genome tags
SELECT COUNT(*) FROM links;           -- 62,423 external links
SELECT COUNT(*) FROM ratings;         -- 25,000,095 user ratings
SELECT COUNT(*) FROM tags;            -- 1,093,360 user-generated tags

-- Verify column names and data types for key tables
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'movies' 
   OR table_name = 'ratings'
ORDER BY table_name, ordinal_position;



/* =======================================================
   SECTION 3: DATA QUALITY ASSESSMENT - YEAR EXTRACTION
   ======================================================= 
   Purpose: Identify and resolve title parsing issues
   Key Findings: 566 movies with year extraction challenges
*/

-- Initial assessment of year extraction failures
SELECT 
    "movieId",
    title,
    SUBSTRING(title FROM '\((\d{4})\)$') as extracted_year
FROM movies 
WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL;

-- Verifying year pattern
SELECT 
    "movieId",
    title,
    -- Check for different year patterns
    CASE 
        WHEN title ~ '\(\d{4}\)' THEN 'has_year_in_parentheses'
        WHEN title ~ '\[\d{4}\]' THEN 'has_year_in_brackets'
        WHEN title ~ '\{\d{4}\}' THEN 'has_year_in_braces'
        WHEN title ~ '\d{4}' THEN 'has_4_digits_somewhere'
        ELSE 'no_4_digits_found'
    END as year_pattern,
    -- Extract what's actually in parentheses if any
    SUBSTRING(title FROM '\((.*)\)') as content_in_parentheses
FROM movies 
WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL
LIMIT 30;

-- Categorize year extraction issues by pattern type
SELECT 
    CASE 
        WHEN title ~ '\(\d{4}\)' THEN 'has_year_in_parentheses'
        WHEN title ~ '\[\d{4}\]' THEN 'has_year_in_brackets' 
        WHEN title ~ '\{\d{4}\}' THEN 'has_year_in_braces'
        WHEN title ~ '\d{4}' THEN 'has_4_digits_but_wrong_format'
        ELSE 'no_4_digits_found'
    END as year_pattern,
    COUNT(*) as movie_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM movies WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL), 2) as percentage
FROM movies 
WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL
GROUP BY year_pattern
ORDER BY movie_count DESC;

/* Results:
   - no_4_digits_found: 406 movies (71.73%) - Genuinely no release year
   - has_year_in_parentheses: 156 movies (27.56%) - Fixable with improved regex
   - has_4_digits_but_wrong_format: 4 movies (0.71%) - Edge cases
*/

-- Investigate fixable cases: movies with years but complex parentheses
SELECT 
    "movieId",
    title,
    SUBSTRING(title FROM '\((.*)\)') as content_in_parentheses
FROM movies 
WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL
  AND title ~ '\(\d{4}\)'
LIMIT 20;

-- Test improved year extraction for complex parentheses
SELECT 
    "movieId",
    title,
    SUBSTRING(title FROM '\((.*)\)') as all_parentheses_content,
    (REGEXP_MATCHES(title, '.*\((\d{4})\)'))[1] as year_from_any_position
FROM movies 
WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL
  AND title ~ '\(\d{4}\)';

-- Verify genuinely missing year data
SELECT "movieId", title
FROM movies 
WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL
  AND title ~ '\(\d{4}\)' = false;

-- Analyze edge cases with non-standard year formats
SELECT 
    "movieId", 
    title,
    (REGEXP_MATCHES(title, '\d{4}'))[1] as found_digits
FROM movies 
WHERE SUBSTRING(title FROM '\((\d{4})\)$') IS NULL
  AND title ~ '\d{4}'               
  AND title ~ '\(\d{4}\)' = false;  

/* YEAR EXTRACTION STRATEGY:
   - 156 movies fixed with improved regex (multiple parentheses)
   - 406 movies correctly remain NULL (no release year in title)  
   - 4 edge cases excluded (TV series, alternative formats)
*/



/* =======================================================
   SECTION 4: RATINGS DATA QUALITY ASSESSMENT
   ======================================================= 
   Purpose: Identify movies without user ratings
   Key Finding: 3,376 movies have zero ratings
*/

-- Identify movies with no rating activity
SELECT 
    m."movieId",
    m.title,
    COUNT(r.rating) as rating_count
FROM movies m
LEFT JOIN ratings r ON m."movieId" = r."movieId"
WHERE r.rating IS NULL
GROUP BY m."movieId", m.title; 



/* =======================================================
   SECTION 5: SUMMARY TABLE CREATION - CORE ANALYTICAL DATASET
   ======================================================= 
   Purpose: Create unified dataset for analysis and visualization
   Design: Genre-exploded schema with calculated metrics and metadata
*/

CREATE TABLE summary_table AS
WITH movie_base AS (
    /* Base movie data with exploded genres and calculated metrics */
    SELECT 
        m."movieId",
        m.title,
        -- Advanced year extraction handling multiple parentheses
        CAST(
            CASE 
                WHEN m.title ~ '.*\((\d{4})\)' 
                THEN (REGEXP_MATCH(m.title, '.*\((\d{4})\)'))[1]
                ELSE NULL
            END AS INTEGER
        ) AS year,
        -- Explode pipe-separated genres into individual rows
        genre,
        -- Rating aggregations
        ROUND(AVG(r.rating)::numeric, 2) AS avg_rating,
        COUNT(r.rating) AS rating_count
    FROM movies m
    CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(m.genres, '|')) AS genre
    LEFT JOIN ratings r ON m."movieId" = r."movieId"
    GROUP BY m."movieId", m.title, genre
),

top_genome AS (
    /* Identify most relevant genome tag for each movie */
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
    /* Identify most frequent user-generated tag per movie */
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

/* Final unified analytical dataset */
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
LEFT JOIN common_user_tag cut ON mb."movieId" = cut."movieId";



/* =======================================================
   SECTION 6: SUMMARY TABLE VALIDATION
   ======================================================= 
   Purpose: Ensure data integrity and completeness
*/

-- Verify movie count preservation
SELECT 
    (SELECT COUNT(*) FROM movies) as original_count,  -- 62423
    (SELECT COUNT(DISTINCT "movieId") FROM summary_table) as summary_count;  -- 62423. i.e, matching with original number of movieId

-- Analyze null rating patterns (expected: 3,376 movies with no ratings)
SELECT 
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE avg_rating IS NULL) as rows_with_null_ratings,
    COUNT(DISTINCT "movieId") FILTER (WHERE avg_rating IS NULL) as movies_with_null_ratings
FROM summary_table;

-- Verify year extraction improvements
SELECT COUNT(DISTINCT "movieId") as movies_with_null_year
FROM summary_table 
WHERE year IS NULL; -- Expected: 410 (improved from 566)

-- Check for genre duplication issues
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
FROM genre_analysis; -- Result: 0



/* =======================================================
   SECTION 7: DATA CLEANING - QUALITY THRESHOLDS
   ======================================================= 
   Purpose: Remove incomplete records for analysis
   Business Rules: Exclude movies without years or ratings
*/

-- Remove movies with missing release years
DELETE FROM summary_table 
WHERE "movieId" IN (
    SELECT DISTINCT "movieId" 
    FROM summary_table 
    WHERE year IS NULL
);

-- Remove movies without any ratings
DELETE FROM summary_table 
WHERE "movieId" IN (
    SELECT DISTINCT "movieId" 
    FROM summary_table 
    WHERE avg_rating IS NULL
);

-- Verify cleaned dataset size
SELECT COUNT(DISTINCT "movieId") as remaining_movies_count
FROM summary_table; -- Result: 58,675 (93.9% of original)



/* =======================================================
   SECTION 8: METADATA COVERAGE ANALYSIS
   ======================================================= 
   Purpose: Assess completeness of genome and user tagging
   Key Insights: Significant opportunity for metadata improvement
*/

-- Genome tag coverage analysis
SELECT 
    COUNT(DISTINCT "movieId") as total_movies,
    COUNT(DISTINCT "movieId") FILTER (WHERE top_genome_tag IS NULL) as movies_missing_genome_tags,
    ROUND(
        (COUNT(DISTINCT "movieId") FILTER (WHERE top_genome_tag IS NULL) * 100.0 / 
         COUNT(DISTINCT "movieId")), 2
    ) as percentage_missing
FROM summary_table;  

-- Genome relevance coverage analysis
SELECT 
    COUNT(DISTINCT "movieId") as total_movies,
    COUNT(DISTINCT "movieId") FILTER (WHERE genome_relevance IS NULL) as movies_missing_genome_relevance,
    ROUND(
        (COUNT(DISTINCT "movieId") FILTER (WHERE genome_relevance IS NULL) * 100.0 / 
         COUNT(DISTINCT "movieId")), 2
    ) as percentage_missing
FROM summary_table;   

-- User tag coverage analysis  
SELECT 
    COUNT(DISTINCT "movieId") as total_movies,
    COUNT(DISTINCT "movieId") FILTER (WHERE most_common_user_tag IS NULL) as movies_missing_most_common_user_tag,
    ROUND(
        (COUNT(DISTINCT "movieId") FILTER (WHERE most_common_user_tag IS NULL) * 100.0 / 
         COUNT(DISTINCT "movieId")), 2
    ) as percentage_missing
FROM summary_table; 

/* METADATA COVERAGE RESULTS:
   - Genome Tags: 44,877 movies missing (76.48%) - Major improvement opportunity
   - User Tags: 16,944 movies missing (28.88%) - Moderate coverage
*/



/* =======================================================
   SECTION 9: DASHBOARD SUPPORT QUERIES
   ======================================================= 
   Purpose: Prepare data for Tableau dashboard visualizations
*/



-- Top movie per decade for historical trends analysis
WITH ranked_movies AS (
  SELECT 
    decade,
    title,
    rating_count,
    avg_rating,
    ROW_NUMBER() OVER (PARTITION BY decade ORDER BY rating_count DESC, avg_rating DESC) as rank
  FROM (
    SELECT 
      FLOOR(year/10)*10 || 's' as decade,
      title,
      MAX(rating_count) as rating_count,
      MAX(avg_rating) as avg_rating
    FROM summary_table 
    GROUP BY "movieId", title, year
  ) deduped
)
SELECT decade, title, rating_count, avg_rating
FROM ranked_movies 
WHERE rank <= 1
ORDER BY decade, rank;

-- Top movie per genre for genre analysis
WITH ranked_movies AS (
  SELECT 
    genre,
    title,
    rating_count,
    avg_rating,
    ROW_NUMBER() OVER (PARTITION BY genre ORDER BY rating_count DESC, avg_rating DESC) as rank
  FROM (
    SELECT 
      genre,
      title,
      MAX(rating_count) as rating_count,
      MAX(avg_rating) as avg_rating
    FROM summary_table 
    GROUP BY "movieId", title, genre
  ) deduped
)
SELECT genre, title, rating_count, avg_rating
FROM ranked_movies 
WHERE rank = 1
ORDER BY genre;

-- Final dataset preview
SELECT * FROM summary_table LIMIT 10;



/* =======================================================
   MOVIELENS ANALYTICAL DATASET READY FOR VISUALIZATION
   ======================================================= 
   Final Dataset: 58,675 movies, genre-exploded, with complete metadata
   Key Features: Year extraction, rating aggregations, genome & user tags
   Ready for: Tableau dashboard, trend analysis, content strategy insights
*/