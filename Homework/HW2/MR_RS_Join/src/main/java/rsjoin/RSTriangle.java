package rsjoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RSTriangle extends Configured implements Tool {
	
	private static final Logger logger = LogManager.getLogger(RSTriangle.class);

	public static class RSMapperEdges extends Mapper<Text, Text, Text, Text> {
		private Text outValue = new Text();
		private Text maxValue = new Text();

		public void setup(Context context) {
			maxValue = new Text(context.getConfiguration().get("MaximumValue"));
		}
		@Override
		public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
			if(Integer.parseInt(key.toString()) < Integer.parseInt(maxValue.toString()) 
					&& Integer.parseInt(value.toString()) < Integer.parseInt(maxValue.toString())){
			outValue.set("E" + value.toString()); // E - edge from edges.csv
			context.write(key, outValue);
			}
		}
	}
	public static class RSMapperPath2 extends Mapper<Text, Text, Text, Text> {
		private Text outValue = new Text();
		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {
			outValue.set("P" + value.toString()); // P - edge from path2
			context.write(key, outValue);
			}
	}

	public static class RSTriangleReducer extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> Edge_List = new ArrayList<Text>();
		private ArrayList<Text> Path2_List = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Clear lists
			Edge_List.clear();
			Path2_List.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with make sure to remove the tag!
			for (Text t : values) {
				if (t.charAt(0) == 'E') {
					Edge_List.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'P') {
					Path2_List.add(new Text(t.toString().substring(1)));
				}
			}

			// If both lists are not empty, join A with B
			if (!Edge_List.isEmpty() && !Path2_List.isEmpty()) {
				for (Text P : Path2_List) {
					for (Text E : Edge_List) {
						if (P.compareTo(E) == 0) {
							context.getCounter(TriangleCounter.TriangleCount).increment(1);
						}
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		conf.set("MaximumValue", "200");
		
		final Job job = Job.getInstance(conf, "RS Triangle");
		job.setJarByClass(RSTriangle.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), KeyValueTextInputFormat.class, RSMapperEdges.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), KeyValueTextInputFormat.class, RSMapperPath2.class);

		job.setReducerClass(RSTriangleReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		if(job.waitForCompletion(true)){
			long counter = job.getCounters().findCounter(TriangleCounter.TriangleCount).getValue()/3;
			logger.info("Triangle Count --> " + counter);
			return 0;
		}
		
		return 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new RSTriangle(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

enum TriangleCounter{
	TriangleCount;
}
