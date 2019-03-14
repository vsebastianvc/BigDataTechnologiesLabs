package AverageTemperature;

import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Descending, Pair>{

	@Override
	public int getPartition(Descending arg0, Pair arg1, int arg2) {
		if (arg2 == 0){
			return 0;
		} else if(Integer.parseInt(arg0.toString()) < 1930){
			return 0;
		} else {
			return 1;
		}
	}

}