--Create table

MOVIES

CREATE TABLE IF NOT EXISTS 
movies( id string,  name string,  genre string) 
ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' ; 

select * from movies limit 10;

LOAD DATA LOCAL INPATH 'movies.csv' INTO TABLE  movies;  


RATINGS:

CREATE TABLE IF NOT EXISTS 
ratings( userid string,  movieid string,  rating string, `timestamp` string ) 
ROW FORMAT DELIMITED  FIELDS TERMINATED BY "," ; 

select * from ratings limit 10;

LOAD DATA LOCAL INPATH 'ratings.csv' INTO TABLE  ratings;  


-- a. To find the movie with the highest average rating.
select movie_id, avg(rating) as avg_rating from ratings
group by movie_id
order by avg_rating desc
limit 1;

-- b. Identify the most active users based on the number of ratings submitted.
select user_id, count(*) as num_ratings from ratings
group by user_id
order by num_ratings desc
limit 10; -- you can adjust the limit as needed

-- c. Discover movies with the highest number of positive ratings.
assuming a positive rating is 4 or 5:
select movie_id, count(*) as positive_ratings from ratings
where rating >= 4
group by movie_id
order by positive_ratings desc
limit 10; -- you can adjust the limit as needed

-- d. Find the top genres ranked by their average rating. 
this step requires joining the movies and ratings tables,
-- explode genres and calculate average ratings for each genre with
exploded_genres as (
select movie_id, explode(split(genres, '[|]')) as genre
from movies ),
genre_ratings as (
select g.genre, avg(r.rating) as avg_rating from exploded_genres g
join ratings r on g.movie_id = r.movie_id group by g.genre
)
select genre, avg_rating from genre_ratings
order by avg_rating desc;