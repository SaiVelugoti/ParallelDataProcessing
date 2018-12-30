package HW2.MR_RepJoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RepJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(RepJoin.class);

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
		

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		conf.set("MaximumValue", "40000");

		final Job job = Job.getInstance(conf, "RS Join");
		job.setJarByClass(RepJoin.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(RSMapperInitial.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new RepJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
