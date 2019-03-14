package AverageTemperature;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Prob2 extends Configured implements Tool
{

	public static class AvgTemperatureMapper extends Mapper<LongWritable, Text, Text, Pair>
	{

		private double temperature;
		private Text year = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
			{
				year.set(token.substring(15, 19));
				temperature = (Double.parseDouble(token.substring(87, 92))/10);
				int cnt = 1;
				Pair p =  new Pair(temperature,cnt);
				context.write(year, p);
			}
		}
	}

	public static class AvgTemperatureCombiner extends Reducer<Text, Pair, Text, Pair>
	{
		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			double temp = 0;
			int cnt = 0;
			for (Pair p : values)
			{
				temp += p.getFirst();
				cnt += p.getSecond();
			}
			Pair combinedPair = new Pair(temp, cnt);
			context.write(key, combinedPair);
		}
	}	
	
	public static class AvgTemperatureReducer extends Reducer<Text, Pair, Text, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			double temp = 0;
			double cnt = 0;
			for (Pair p : values)
			{
				temp += p.getFirst();
				cnt += p.getSecond();
			}
			result.set(temp/cnt);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
				FileSystem fs = FileSystem.get(conf);
				fs.delete(new Path(args[1]), true);

		int res = ToolRunner.run(conf, new Prob2(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(Prob2.class);

		job.setMapperClass(AvgTemperatureMapper.class);
		job.setCombinerClass(AvgTemperatureCombiner.class);
		job.setReducerClass(AvgTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Pair.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
