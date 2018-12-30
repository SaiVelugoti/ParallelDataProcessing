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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RSJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(RSJoin.class);

	public static class RSMapperInitial extends Mapper<Text, Text, Text, Text> {
		private Text outValue = new Text();
		private Text maxValue = new Text();

		public void setup(Context context) {
			maxValue = new Text(context.getConfiguration().get("MaximumValue"));
		}

		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {
			if(Integer.parseInt(key.toString()) < Integer.parseInt(maxValue.toString()) 
					&& Integer.parseInt(value.toString()) < Integer.parseInt(maxValue.toString())){
				outValue.set("F" + value.toString());
				context.write(key, outValue);
				outValue.set("T" + key.toString());
				context.write(value, outValue);
			}
		}
	}

	public static class RSReducer extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> From_List = new ArrayList<Text>();
		private ArrayList<Text> To_List = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Clear lists
			From_List.clear();
			To_List.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with make sure to remove the tag!
			for (Text t : values) {
				if (t.charAt(0) == 'F') {
					From_List.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'T') {
					To_List.add(new Text(t.toString().substring(1)));
				}
			}

			// If both lists are not empty, join A with B
			if (!To_List.isEmpty() && !From_List.isEmpty()) {
				for (Text T : To_List) {
					for (Text F : From_List) {
						if (T.compareTo(F) != 0) {
							// To print Path 2
							// outValue.set(key + "\t" + F);
							// context.write(T, outValue);

							// Required Edge to complete triangle
							context.write(F, T);
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

		final Job job = Job.getInstance(conf, "RS Join");
		job.setJarByClass(RSJoin.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(RSMapperInitial.class);
		job.setReducerClass(RSReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <intermediate-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new RSJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
