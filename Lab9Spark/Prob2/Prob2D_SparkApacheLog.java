package cs523.ApacheLog;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.text.ParseException;


public class Prob2D_SparkApacheLog 
{
	public static void main( String[] args ) throws IllegalArgumentException, IOException, ParseException
    { 	   	
		//Automatic removal directory before job execution
    	Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("output_prob2d"), true);
				
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("apacheLog").setMaster("local"));
		JavaRDD<String> lines = sc.textFile(args[0]);
		
	    JavaPairRDD<String, Integer> ipList = lines
	    	.flatMap(line -> Arrays.asList(line.split("\\r?\\n")))
	    	.map(w -> new StringTokenizer(w))
	    	.map(w -> w.nextToken())
        .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
        .reduceByKey((x, y) -> x + y)
        .filter(w -> (w._2 > 20));
	    			 
	    ipList.saveAsTextFile(args[1]);

		sc.close();		
		}
}
