users = LOAD '/home/cloudera/Desktop/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
maleLawyerFilter = FILTER users BY gender=='M' AND occupation=='lawyer';
groupByLawyer = GROUP maleLawyerFilter BY 'occupation';
countMaleLawyer = FOREACH groupByLawyer GENERATE COUNT(maleLawyerFilter);
STORE countMaleLawyer INTO '/home/cloudera/Desktop/output_prob1';
