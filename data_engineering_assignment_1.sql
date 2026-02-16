
-- Data loading
create or replace table raw_movies as
select *
from read_json_auto(
        '/Users/dianaa/Documents/AI28 - 2/data_engineering/assign_1/collection.json',
        maximum_object_size=200000000);

select * from raw_movies;




-- Data parsing
create or replace table movies as
select
    json_extract_string(raw_movies, '$._id.$oid')      AS movie_id,
    json_extract_string(raw_movies, '$.title')      AS title,
    CAST(json_extract(raw_movies, '$.year.$numberInt') AS INTEGER) AS year,
    CAST(json_extract(raw_movies, '$.runtime.$numberInt') AS INTEGER) AS runtime,
    array_to_string(json_extract(raw_movies, '$.cast')::VARCHAR[],', ') AS cast,
    plot,
    lastupdated as last_updated,
    array_to_string(json_extract(raw_movies, '$.directors')::VARCHAR[],', ') AS directors,
    CAST(json_extract(raw_movies, '$.imdb.rating.$numberDouble') AS DOUBLE) AS imdb_rating,
    CAST(json_extract(raw_movies, '$.imdb.votes.$numberInt') AS INTEGER) AS imdb_num_votes,
    array_to_string(json_extract(raw_movies, '$.countries')::VARCHAR[],', ') AS countries,
    array_to_string(json_extract(raw_movies, '$.genres')::VARCHAR[],', ') AS genres,
    CAST(json_extract(raw_movies, '$.tomatoes.viewer.rating.$numberDouble') AS DOUBLE) AS tomatoes_rating,
    CAST(json_extract(raw_movies, '$.num_mflix_comments.$numberInt') AS INTEGER) AS mflix_num_comments,
    array_to_string(json_extract(raw_movies, '$.writers')::VARCHAR[],', ') AS writers,
    array_to_string(json_extract(raw_movies, '$.languages')::VARCHAR[],', ') AS languages
from raw_movies;

select * from movies;




-- Data analysis

-- Query 1:
-- For each movie director show top 3 movies with the highest imdb_rating
with prep_query as (
    select
        director,
        title,
        imdb_rating
    from movies,
         unnest(string_split(directors, ', ')) AS t(director)
)
select
    director,
    rank() over (partition by director order by imdb_rating desc) as ranki,
    title,
    imdb_rating
from prep_query
qualify rank() over (partition by director order by imdb_rating desc) <= 3
order by director, ranki;



-- Query 2:
-- For each pair (year, genre) give number of movies released and average imdb_rating,
-- and compare the latter to average imd_rating for that year alone
with prep_query as (
    select
        year,
        genre,
        imdb_rating
    from movies,
        unnest(string_split(genres, ', ')) AS t(genre)
    where imdb_rating is not null
)
select distinct year,
    genre,
    round(avg(imdb_rating) over (partition by year, genre), 2) as avg_imdb_rating_year_genre,
    round(avg(imdb_rating) over (partition by year), 2) as avg_imdb_rating_year,
    count() over (partition by year, genre) as num_movies_year_genre
from prep_query
order by year desc, genre;








