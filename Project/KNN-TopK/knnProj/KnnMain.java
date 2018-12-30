package knnProj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import knnProj.HelperClasses.DistClass;
import knnProj.PredictClass.KNN_Multi_Mapper;
import knnProj.PredictClass.KNN_Multi_Reducer;
import knnProj.Test_IDGen.GenerateIDMapper;
import knnProj.Test_IDGen.GenerateIDReducer;
import knnProj.PredOutputFormat.OutputFormatterMapper;
import knnProj.Metrics.Metrics_Mapper;
import knnProj.MetricCounters;

public class KnnMain extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(KnnMain.class);

	public static void main(final String[] args) {
		if (args.length != 7) {
			throw new Error(
					"Six arguments required:\n<k-value> <train-data> <test-data> <testDataID> <intermediateOP> <output>");
		}
		try {
			ToolRunner.run(new KnnMain(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

	public int run(String[] args) throws Exception {

		int jobStatus = 0;

		// ---------------------------------------------------------------------------------
		// ---------------------------------------------------------------------------------
		// Generate ID for each test record
		final Configuration conf_idGen = getConf();
		conf_idGen.set("idStart", "10000");

		final Job job_idGen = Job.getInstance(conf_idGen, "TestData ID");
		job_idGen.setJarByClass(Test_IDGen.class);

		final Configuration jobConf_idGen = job_idGen.getConfiguration();
		jobConf_idGen.set("mapreduce.output.textoutputformat.separator", "\t");

		job_idGen.setMapperClass(GenerateIDMapper.class);
		job_idGen.setReducerClass(GenerateIDReducer.class);
		job_idGen.setNumReduceTasks(1);
		job_idGen.setOutputKeyClass(Text.class);
		job_idGen.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job_idGen, new Path(args[2]));
		FileOutputFormat.setOutputPath(job_idGen, new Path(args[3]));

		jobStatus = job_idGen.waitForCompletion(true) ? 0 : 1;

		// --------------------------------------------------------------------------------
		// ---------------------------------------------------------------------------------
		// Calculate distance between each test record and each training record
		final Configuration conf = getConf();
		conf.set("k", args[0]);
		conf.set("testDataPath", args[3]);
		conf.set("minAmount", "0");
		conf.set("maxAmount", "92445516");

		final Job job = Job.getInstance(conf, "KNN Classification");
		job.setJarByClass(PredictClass.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		job.setMapperClass(KNN_Multi_Mapper.class);
		job.setReducerClass(KNN_Multi_Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DistClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		FileSystem fs = FileSystem.get(new Path(args[3]).toUri(), conf);
		Path fsInput = new Path(args[3]);
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(fsInput, true);
		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			job.addCacheFile(fileStatus.getPath().toUri());
		}

		jobStatus = job.waitForCompletion(true) ? 0 : 1;

		// ---------------------------------------------------------------------------------
		// ---------------------------------------------------------------------------------
		// Format Output
		final Configuration conf1 = getConf();
		conf1.set("testDataPath", args[3]);
		conf1.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

		final Job job1 = Job.getInstance(conf1, "KNN");
		job1.setJarByClass(PredOutputFormat.class);

		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		job1.setMapperClass(OutputFormatterMapper.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[4]));
		FileOutputFormat.setOutputPath(job1, new Path(args[5]));

		FileSystem fs1 = FileSystem.get(new Path(args[3]).toUri(), conf1);
		Path fsInput1 = new Path(args[3]);
		RemoteIterator<LocatedFileStatus> fileStatusListIterator1 = fs1.listFiles(fsInput1, true);
		while (fileStatusListIterator1.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator1.next();
			job1.addCacheFile(fileStatus.getPath().toUri());
		}

		jobStatus = job1.waitForCompletion(true) ? 0 : 1;

		// ---------------------------------------------------------------------------------
		// ---------------------------------------------------------------------------------
		// Calculate Metrics
		final Configuration metric_conf = getConf();
		metric_conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		
		final Job job_metrics = Job.getInstance(metric_conf, "TestData ID");
		job_metrics.setJarByClass(Metrics.class);

		final Configuration jobConf_metrics = job_metrics.getConfiguration();
		jobConf_metrics.set("mapreduce.output.textoutputformat.separator", "\t");

		job_metrics.setInputFormatClass(KeyValueTextInputFormat.class);
		job_metrics.setMapperClass(Metrics_Mapper.class);
		job_metrics.setOutputKeyClass(Text.class);
		job_metrics.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job_metrics, new Path(args[5]));
		FileOutputFormat.setOutputPath(job_metrics, new Path(args[6]));

		if (job_metrics.waitForCompletion(true)) {
			logger.info("TP -> "+job_metrics.getCounters().findCounter(MetricCounters.TP).getValue());
			logger.info("FP -> "+job_metrics.getCounters().findCounter(MetricCounters.FP).getValue());
			logger.info("TN -> "+job_metrics.getCounters().findCounter(MetricCounters.TN).getValue());
			logger.info("FN -> "+job_metrics.getCounters().findCounter(MetricCounters.FN).getValue());
		}
		return jobStatus;
	}

}
