package knnProj;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Test_IDGen {

	public static class GenerateIDMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			context.write(new Text("0"), value);
		}
	}

	public static class GenerateIDReducer extends Reducer<Text, Text, Text, Text> {
		private Integer k;

		@Override
		protected void setup(Context context) {
			k = Integer.parseInt(context.getConfiguration().get("idStart"));
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(new Text((++k).toString()), t);
			}
		}
	}
}
