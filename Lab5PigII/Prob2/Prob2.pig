users = LOAD '/home/cloudera/Desktop/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
maleLawyerFilter = FILTER users BY gender=='M' AND occupation=='lawyer';
OrderByLawyer = ORDER maleLawyerFilter BY age DESC;
usersIdColumn = FOREACH OrderByLawyer GENERATE userId;
oldest = LIMIT usersIdColumn 1;
STORE oldest INTO '/home/cloudera/Desktop/output_prob2';
