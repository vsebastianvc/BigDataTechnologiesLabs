package AverageTemperature;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import AverageTemperature.*;


public class Prob5 extends Configured implements Tool
{

	public static class AvgTemperatureMapper extends Mapper<LongWritable, Text, Descending, Pair>
	{

		private Double temp;
		private Integer cnt = new Integer(1);
		private Text year = new Text();
		Map<String,Double> mapTemp = new HashMap<String,Double>();
		Map<String,Integer> mapCnt = new HashMap<String,Integer>();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			for (String token : value.toString().split("\\s+"))
			{
				year.set(token.substring(15, 19));
				temp = (Double.parseDouble(token.substring(87, 92))/10);
				mapTemp.put(year.toString(), ((mapTemp.get(year.toString()) == null)? temp : mapTemp.get(year.toString())+temp));
				mapCnt.put(year.toString(), ((mapCnt.get(year.toString()) == null)? 1 : mapCnt.get(year.toString())+cnt));
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{	
			for(String y : mapTemp.keySet()){
				Pair p = new Pair(mapTemp.get(y), mapCnt.get(y));
				context.write(new Descending(y), p);
			}			
		}
	}

	
	public static class AvgTemperatureReducer extends Reducer<Descending, Pair, Descending, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Descending key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			double temp = 0;
			int cnt = 0;
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

		//Automatic removal of “output” directory before job execution
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);


		int res = ToolRunner.run(conf, new Prob5(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AverageTemperature");
		job.setJarByClass(Prob5.class);

		job.setMapperClass(AvgTemperatureMapper.class);
		job.setReducerClass(AvgTemperatureReducer.class);

		job.setOutputKeyClass(Descending.class);
		job.setOutputValueClass(Pair.class);
		
		job.setNumReduceTasks(2);
		job.setPartitionerClass(CustomPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
}


