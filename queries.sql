-- 1. Movie with highest average rating
SELECT m.title, AVG(r.rating) AS avg_rating
FROM ratings r
JOIN movies m ON r.movieId = m.movieId
GROUP BY r.movieId
ORDER BY avg_rating DESC
LIMIT 1;

-- 2. Top 5 genres with highest average rating
SELECT
  g.genre,
  AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r USING (movieId)
JOIN JSON_TABLE(
       CONCAT('["', REPLACE(m.genres, '|', '","'), '"]'),
       '$[*]' COLUMNS (genre VARCHAR(100) PATH '$')
     ) AS g
  ON TRUE
WHERE m.genres IS NOT NULL
  AND m.genres <> '(no genres listed)'
GROUP BY g.genre
ORDER BY avg_rating DESC
LIMIT 5;


-- 3. Director with most movies
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director <> ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. Average rating per release year
SELECT m.year, AVG(r.rating) AS avg_rating
FROM ratings r
JOIN movies m ON r.movieId = m.movieId
GROUP BY m.year
ORDER BY m.year;
