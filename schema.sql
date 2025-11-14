use movie_db;
CREATE TABLE IF NOT EXISTS movies (
    movieId INT PRIMARY KEY,
    title VARCHAR(255),
    genres VARCHAR(255),
	director VARCHAR(255),
	plot TEXT,
	box_office VARCHAR(100),
	year INT,
	imdb_id VARCHAR(30),
	last_enriched_at DATETIME
);


drop table ratings;



CREATE TABLE IF NOT EXISTS ratings (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  userId INT NOT NULL,
  movieId INT NOT NULL,
  rating DECIMAL(3,2) NOT NULL,
  timestamp DATETIME NULL,
  UNIQUE KEY uq_user_movie (userId, movieId),
  FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


select*from ratings;
select*from movies;
USE movie_db;
SHOW COLUMNS FROM movies;

drop table movies;
drop table ratings;