package Project.FraudDetection.KNN;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;



public class KNN extends Configured implements Tool  {

	private static final Logger logger = LogManager.getLogger(KNN.class);
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length != 2) {
			throw new Error("Two arguments are required specifying the input and output directory ");
		}
		try {
			ToolRunner.run(new KNN(), args);
		} catch(final Exception e) {
			logger.error("",e);
		}

	}
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
		// Adding the k value to the configuration
//		conf.set("KValue", args[2]);
		final Job job = Job.getInstance(conf,"KNN");
		job.setJarByClass(KNN.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		
		job.setMapperClass(MaxAmountMapper.class);
		job.setReducerClass(MaxAmountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int jobStatus = job.waitForCompletion(true) ? 0 : 1;	
		if(jobStatus == 0) {
		long maxAmt=job.getCounters().findCounter(MaxMinVal.MaxAmt).getValue();
		long minAmt=job.getCounters().findCounter(MaxMinVal.MinAmt).getValue();
		logger.info("--------------Max Amt---------"+(maxAmt));
		logger.info("--------------Min Amt---------"+(minAmt));
		conf.set("MaxAmt", maxAmt+"");
		conf.set("MinAmt", minAmt+"");
		}
		return jobStatus;
	}
	
	public static class MaxAmountMapper extends Mapper<Object, Text, Text, IntWritable>{
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {			
			String[] splitTokens = value.toString().split(",");
			double amt = Double.parseDouble(splitTokens[2]);
			int amount = (int) amt;
			context.write(new Text("dummy"), new IntWritable(amount));
		}
	}
	
	public static class MaxAmountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(final Text key, final Iterable<IntWritable> values,final Context context) throws IOException, InterruptedException{

			Integer minValue = Integer.MAX_VALUE;
			Integer maxValue = Integer.MIN_VALUE;
			
			for(IntWritable val: values) {
				Integer valType = Integer.parseInt(val.toString());
				if(maxValue< valType){
					maxValue = valType;
				}
				if(minValue > valType) {
					minValue = valType;
				}
			}
			context.getCounter(MaxMinVal.MaxAmt).setValue(maxValue);
			context.getCounter(MaxMinVal.MinAmt).setValue(minValue);
		}
	}
}
