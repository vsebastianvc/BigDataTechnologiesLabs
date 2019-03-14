package cs523.ApacheLog;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;  
import java.util.Date;


public class Prob2C_SparkApacheLog 
{
	public static void main( String[] args ) throws IllegalArgumentException, IOException, ParseException
    { 	   	
		//Automatic removal of “output_prob2c” directory before job execution
    	Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("output_prob2c"), true);
		
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please enter the FIRST date of the range:");
		System.out.println("Format: dd/MMM/yyyy (Ex. 06/Mar/2004) - Open Interval");
		Date from = new SimpleDateFormat("dd/MMM/yyyy").parse(scanner.next());
		System.out.println("Please enter the LAST date of the range:");
		System.out.println("Format: dd/MMM/yyyy (Ex. 12/Mar/2004) - Open Interval");
		Date to = new SimpleDateFormat("dd/MMM/yyyy").parse(scanner.next());
		scanner.close();
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("apacheLog").setMaster("local"));
		JavaRDD<String> lines = sc.textFile(args[0]);
	    JavaPairRDD<String, Integer> ipList = lines
	    	.flatMap(line -> Arrays.asList(line.split("\\r?\\n")))
	    	.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 2282488149569976486L;
			
			@Override
			public Boolean call(String s) throws Exception {
				StringTokenizer matcher = new StringTokenizer(s);
				matcher.nextToken();
				matcher.nextToken();
				matcher.nextToken();
				matcher.nextToken("]");
				matcher.nextToken("\"");
				matcher.nextToken();
				matcher.nextToken(" ");
				return (matcher.nextToken().equals("401"));
			}

        })
        .filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 3263491685115121779L;

			@Override
			public Boolean call(String s) throws Exception {
				StringTokenizer matcher = new StringTokenizer(s);
			    matcher.nextToken();
			    matcher.nextToken();
			    Date currentDate = new SimpleDateFormat("dd/MMM/yyyy").parse(matcher.nextToken("]").substring(4, 24));
			    return ((currentDate.after(from)) && (currentDate.before(to)));
			}})
        .mapToPair(w -> new Tuple2<String, Integer>("Total 401 responses from: "+from+" to: "+to, 1))
        .reduceByKey((x, y) -> x + y);    			
	    			 
	    ipList.saveAsTextFile(args[1]);

		sc.close();		
		}
}
