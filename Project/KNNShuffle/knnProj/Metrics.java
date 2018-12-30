package knnProj;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Metrics {

	public static class Metrics_Mapper extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {

			Integer actual = Integer.parseInt(key.toString().split(",")[9]);
			Integer predicted = Integer.parseInt(value.toString());

			if(predicted == 0)
				if(actual == 0)
					context.getCounter(MetricCounters.TN).increment(1);
				else
					context.getCounter(MetricCounters.FN).increment(1);
			else if (predicted == 1)
				if (actual == 1)
					context.getCounter(MetricCounters.TP).increment(1);
				else
					context.getCounter(MetricCounters.FP).increment(1);
		}
	}
}
