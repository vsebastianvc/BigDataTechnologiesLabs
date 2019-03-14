words = LOAD '/home/cloudera/Desktop/Lab2-WC-Input.txt' AS (line:chararray);
tokenBag = FOREACH words GENERATE TOKENIZE(line); 
flatBag = FOREACH tokenBag GENERATE flatten($0) as word;
charGroup = GROUP flatBag by word;
counts = FOREACH charGroup GENERATE group, COUNT(flatBag);
dump counts
