Movie Data Pipeline – Data Engineering Assignment

This project implements a complete Movie Data Pipeline as part of a Data Engineering assignment.
It processes movie metadata and user ratings from the MovieLens dataset, enriches the movie information using the OMDb API, transforms the data, and loads it into a MySQL relational database.

The final dataset is then used to answer analytical questions through SQL queries.

Project Structure
your-repo/
│── etl.py
│── schema.sql
│── queries.sql
│── README.md
│── movies.csv
│── ratings.csv
│── omdb_cache.json
└── .env   (not included in GitHub — must be created locally)


1. Environment Setup
1.1 Clone or download the repository
git clone <your-repo-link>
cd movie-data-pipeline

1.2 Install dependencies
pip install -r requirements.txt

1.3 Create a .env file
Create a file named .env in your project folder with the following content:
MYSQL_HOST=127.0.0.1
MYSQL_USER=root
MYSQL_PASS=your_mysql_password
MYSQL_DB=movie_db
OMDB_API_KEY=your_omdb_api_key


2. Database Setup
2.1 Login to MySQL
mysql -u root -p

2.2 Create the database
CREATE DATABASE movie_db;
USE movie_db;

2.3 Execute the schema
SOURCE schema.sql;

This creates all required tables such as movies, ratings, and optionally movie_genres.


3. Understanding the ETL Script (etl.py)

The ETL script performs three main steps: Extract, Transform, and Load.

Extract:

Reads movies.csv
Reads ratings.csv
Loads cached OMDb data (omdb_cache.json) if available

Transform:
Cleans movie titles and extracts release years
Handles missing values
Validates and converts timestamps
Splits or normalizes genres (optional)
Fetches enriched movie details from the OMDb API with retry + caching

Load:
Inserts processed movies and ratings into MySQL
Uses upserts to avoid duplicate entries
Saves enrichment metadata


4. Running the ETL Pipeline
4.1 Ensure prerequisites are ready

MySQL server is running
.env file is configured
Schema is created
Dependencies installed

4.2 Run the ETL
python etl.py

This will:
Load movies data
Load ratings data
Perform OMDb enrichment
Insert all records into MySQL

4.3 Useful Command Variants
Run ETL without OMDb enrichment
python etl.py --no-enrich

Process limited rows (for testing)
python etl.py --limit 500

Recreate the ratings table
python etl.py --recreate-ratings

4.4 Verify Data Load

Open MySQL and run:
USE movie_db;
SELECT COUNT(*) FROM movies;
SELECT COUNT(*) FROM ratings;


5. Analytical SQL Queries (queries.sql)

The following questions are answered using SQL:
Movie with the highest average rating
Top 5 genres with the highest average rating
Director with the most movies
Average rating per year
 

6. Design Decisions & Assumptions

OMDb caching is used to avoid repeated API calls
ETL is idempotent using ON DUPLICATE KEY UPDATE
Genres can be stored as pipe-separated or normalized into a separate table
UNIX timestamps are safely converted to MySQL DATETIME
OMDb search fallback handles mismatched titles
Tables include indexing for faster analytics


7. Challenges and Solutions

Timestamp inconsistencies
Converted UNIX timestamps into proper MySQL datetime formats.
Missing OMDb data
Handled gracefully by inserting NULL values.
OMDb API rate limits
Used caching and retry logic to prevent failures.
Duplicate loading
Handled using idempotent SQL operations.


8. Future Improvements

Convert this pipeline into an Airflow DAG
Add Docker support for consistent deployment
Store raw/enriched data in cloud storage (e.g., AWS S3)
Use Apache Spark for large-scale transformations
Build BI dashboards using Superset or PowerBI