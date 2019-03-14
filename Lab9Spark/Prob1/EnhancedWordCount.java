package cs523.SparkWC;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class EnhancedWordCount
{

	public static void main(String[] args) throws Exception
	{
		Scanner scanner = new Scanner(System.in);
		System.out.print("Please enter the word frequency threshold:");
		int threshold = scanner.nextInt();
		scanner.close();

		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<String, Integer> counts = lines
					.flatMap(line -> Arrays.asList(line.split(" ")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y)
					.filter(w -> (w._2 < threshold))
					.flatMap(w -> Arrays.asList(w._1.toLowerCase().split("(?!^)")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y);

		// Save the word count into a textfile
		counts.saveAsTextFile(args[1]);

		sc.close();
	}
}
