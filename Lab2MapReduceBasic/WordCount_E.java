
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount_E extends Configured implements Tool
{

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		HashMap<String, Integer> wordCounts;
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		

		protected void setup(Context context) throws IOException, InterruptedException {
			//Only declare the HashSet
			wordCounts = new HashMap<>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//Write a program to find out how many unique words are there in the input file 
			//This program count word with upper case and lower case as the same word
			//This program doesn't count special characters and symbols
			for (String token : value.toString().replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+")) {
				if (!wordCounts.containsKey(token) && !token.isEmpty()) {
					wordCounts.put(token, 1);
					word.set("");
					context.write(word, one);
				}
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();	
		private int unique=0;
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			for (IntWritable val : values)
			{
				unique += val.get();
			}
			result.set(unique);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);

		int res = ToolRunner.run(conf, new WordCount_E(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount_E.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}

