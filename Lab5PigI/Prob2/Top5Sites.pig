users = LOAD '/home/cloudera/Desktop/users.csv' USING PigStorage(',') AS (user:chararray, age:int);
pages = LOAD '/home/cloudera/Desktop/pages.csv' USING PigStorage(',') AS (user:chararray, address:chararray);
usersFiltered = FILTER users by (age>=18) AND (age<=25);
joinOnName = JOIN usersFiltered BY user, pages BY user;
groupByUrl = GROUP joinOnName by address;
urlCounts = FOREACH groupByUrl GENERATE group, COUNT(joinOnName);
sortedCount = ORDER urlCounts by $1 DESC;
top5 = LIMIT sortedCount 5;
dump top5;




