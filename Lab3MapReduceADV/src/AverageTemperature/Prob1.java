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

public class Prob1 extends Configured implements Tool
{

	public static class AvgTemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{

		private Text year = new Text();
		private DoubleWritable temperature;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
			{
			year.set(token.toString().substring(15, 19));
			temperature = new DoubleWritable(Double.parseDouble(token.toString().substring(87, 92))/10);
			context.write(year, temperature);
			}
		}
	}

	public static class AvgTemperatureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double sum=0;
			int count=0;
			
			for (DoubleWritable val : values) {
				sum+=val.get();
				count++;
			}
			result.set(sum/count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		//Automatic removal of “output” directory before job execution
				FileSystem fs = FileSystem.get(conf);
				fs.delete(new Path(args[1]), true);


		int res = ToolRunner.run(conf, new Prob1(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(Prob1.class);

		job.setMapperClass(AvgTemperatureMapper.class);
		job.setReducerClass(AvgTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
