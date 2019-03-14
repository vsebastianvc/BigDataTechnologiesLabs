REGISTER /home/cloudera/Desktop/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
movies = LOAD 'Desktop/movies.csv' USING CSVLoader(',') AS (movieId:int, title:chararray, genres:chararray);
tokenGenres = FOREACH movies GENERATE movieId,title, TOKENIZE(genres,'|');
flatGenres = FOREACH tokenGenres GENERATE movieId,title, FLATTEN($2) AS genre;
aventureFilter = FILTER flatGenres BY (genre == 'Adventure');
rates = LOAD 'Desktop/rating.txt' AS (userId:int, movieId:int, rating:int, timestamp:int);
ratesFilter = FILTER rates BY rating == 5;
ratesGroup= GROUP ratesFilter BY movieId;
ratesDistinct = FOREACH ratesGroup GENERATE group, 5 AS maxScore;
moviesRating = JOIN aventureFilter BY movieId, ratesDistinct BY $0;
moviesRatingFilter = FOREACH moviesRating GENERATE movieId, genre, maxScore, title;
moviesRatingOrder = ORDER moviesRatingFilter BY movieId;
top20Movies = limit moviesRatingOrder 20;
STORE top20Movies INTO '/home/cloudera/Desktop/output_prob4';
