package MRFilter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MaxFilter extends Configured implements Tool{
	private static final Logger logger = LogManager.getLogger(MaxFilter.class);	

	public static class FilterMapper extends Mapper<Text, Text, Text, Text> {
		
		private Text maxValue = new Text();

		public void setup(Context context){
			logger.info("--------maxValue"+maxValue);
			maxValue = new Text(context.getConfiguration().get("MaximumValue"));
			logger.info("--------");
		}
		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {
			
		logger.info("Key: "+key+"\t Value: "+maxValue+"\t key.compareTo(maxValue) -> "+key.compareTo(maxValue));
		logger.info("Key: "+value+"\t Value: "+maxValue+"\t value.compareTo(maxValue) -> "+value.compareTo(maxValue));
		if(key.compareTo(maxValue) < 0 && value.compareTo(maxValue) < 0){
			context.write(key, value);
		}
	}
	}
		@Override
		public int run(final String[] args) throws Exception {
			final Configuration conf = getConf();
			conf.set("MaximumValue", "6");
			conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

			final Job job = Job.getInstance(conf, "Max Filter");
			job.setJarByClass(MaxFilter.class);

			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", ",");

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setMapperClass(FilterMapper.class);

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
			ToolRunner.run(new MaxFilter(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}