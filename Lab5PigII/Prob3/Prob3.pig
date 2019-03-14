REGISTER /home/cloudera/Desktop/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
movies = LOAD 'Desktop/movies.csv' USING CSVLoader(',') AS (movieId:int, title:chararray, genres:chararray);
titleFilter = FILTER movies BY STARTSWITH(title,'a') OR STARTSWITH(title,'A');
tokenGenres = FOREACH titleFilter GENERATE TOKENIZE(genres,'|');
flatGenres = FOREACH tokenGenres GENERATE FLATTEN($0) AS genre;
groupByGenres = GROUP flatGenres BY $0;
totalGenres = FOREACH groupByGenres GENERATE group, COUNT(flatGenres.genre);
STORE totalGenres INTO '/home/cloudera/Desktop/output_prob3';
