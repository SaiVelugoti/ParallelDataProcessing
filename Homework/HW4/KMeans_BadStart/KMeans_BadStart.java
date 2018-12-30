package kMeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
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

public class KMeans_BadStart extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(KMeans_BadStart.class);

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <output-dir> <centroid> <k_value>");
		}
		try {
			ToolRunner.run(new KMeans_BadStart(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		// Job 1
		final Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		final Job job = Job.getInstance(conf, "K Means");
		job.setJarByClass(KMeans_BadStart.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(FollowerMapper.class);
		job.setCombinerClass(FollowerReducer.class);
		job.setReducerClass(FollowerReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int jobStatus = job.waitForCompletion(true) ? 0 : 1;

		// Job 2 - Initial Centroid Creation

		final Configuration confCent = getConf();
		confCent.set("k_value", args[3]);
		confCent.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		final Job jobCent = Job.getInstance(confCent, "K Means");
		jobCent.setJarByClass(KMeans_BadStart.class);

		final Configuration jobConfCentr = jobCent.getConfiguration();
		jobConfCentr.set("mapreduce.output.textoutputformat.separator", ",");

		jobCent.setInputFormatClass(KeyValueTextInputFormat.class);
		jobCent.setMapperClass(InitCentroidMapper.class);
		jobCent.setReducerClass(InitCentroidReducer.class);

		jobCent.setOutputKeyClass(Text.class);
		jobCent.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobCent, new Path(args[1]));
		FileOutputFormat.setOutputPath(jobCent, new Path(args[2] + "_0"));
		jobStatus = jobCent.waitForCompletion(true) ? 0 : 1;

		// Job 3

		Integer iteration = 1;
		Boolean jobDone = false;
		long old_SSE = (long) 0.0;
		long new_SSE = (long) 0.0;
		long final_SSE = (long) 0.0;

		String iterationOutput = args[2] + "_" + iteration;
		String centroidsPath = args[2] + "_0/";

		while (!jobDone && iteration <= 10) {

			final Configuration conf1 = getConf();
			conf1.set("centroid_Path", centroidsPath);
			conf1.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

			final Job job1 = Job.getInstance(conf1, "K Means");
			job1.setJarByClass(KMeans_BadStart.class);

			final Configuration jobConf1 = job1.getConfiguration();
			jobConf1.set("mapreduce.output.textoutputformat.separator", ",");

			job1.setInputFormatClass(KeyValueTextInputFormat.class);
			job1.setMapperClass(CentroidMapper.class);
			job1.setReducerClass(CentroidReducer.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(iterationOutput));

			FileSystem fs = FileSystem.get(new Path(centroidsPath).toUri(), conf);
			Path fsInput = new Path(centroidsPath);
			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(fsInput, true);
			while (fileStatusListIterator.hasNext()) {
				LocatedFileStatus fileStatus = fileStatusListIterator.next();
				job1.addCacheFile(fileStatus.getPath().toUri());
			}

			jobStatus = job1.waitForCompletion(true) ? 0 : 1;

			new_SSE = job1.getCounters().findCounter(New_SSE.new_SSE).getValue();

			if (iteration == 1) {
				logger.info("iteration --> " + iteration + "-> new_SSE ->" + new_SSE + "-> old_SSE ->" + old_SSE);
				old_SSE = new_SSE;
				iteration = iteration + 1;
				centroidsPath = iterationOutput + "/";
				iterationOutput = args[2] + "_" + iteration;
			} else if (Math.abs(new_SSE - old_SSE) < 0.01) {
				final_SSE = old_SSE;
				jobDone = true;
				logger.info("iteration --> " + iteration + "-> final_SSE ->" + final_SSE);
				return jobStatus;
			} else if (Math.abs(new_SSE - old_SSE) > 0.01) {
				logger.info("iteration --> " + iteration + "-> new_SSE ->" + new_SSE + "-> old_SSE ->" + old_SSE);
				old_SSE = new_SSE;
				jobDone = false;
				iteration = iteration + 1;
				centroidsPath = iterationOutput + "/";
				iterationOutput = args[2] + "_" + iteration;
			}
		}
		return jobStatus;

	}

	// Mapper - 1 - Follower Count
	public static class FollowerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		// when each line is parsed as (key, value) pair,
		// key gives the follower and value gives the user who is being followed
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			context.write(value, one);
		}
	}

	// Reducer - 1 - Follower Count
	public static class FollowerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// Mapper - 1 - Initialize Centroid
	public static class InitCentroidMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {

			context.write(new Text("dummy"), new Text(value.toString()));
		}
	}

	// Reducer - 1 - Initialize Centroid
	public static class InitCentroidReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context)
				throws IOException, InterruptedException {
			Integer max_follower = 0;
			Integer min_follower = Integer.MAX_VALUE;
			Set<Integer> followerCount = new HashSet<Integer>();
			for (Text val : values) {
				followerCount.add(Integer.parseInt(val.toString()));
			}
			for (Integer i : followerCount) {
				if (i > max_follower) {
					max_follower = i;
				}
				if (i < min_follower) {
					min_follower = i;
				}
			}
			Integer k = Integer.parseInt(context.getConfiguration().get("k_value"));

			// Bad Start
			Integer centroid = min_follower;
			for (int i = 1; i <= k; i++) {
				centroid = centroid + 100;
				context.write(new Text(centroid.toString()), new Text("0"));
			}
		}
	}

	// Mapper - 2 - Centroid Loop
	public static class CentroidMapper extends Mapper<Text, Text, Text, Text> {

		ArrayList<Double> centroids = new ArrayList<Double>();

		@Override
		public void setup(Context context) throws IOException {
			String centroid_Path = context.getConfiguration().get("centroid_Path");
			URI[] uris = context.getCacheFiles();
			if (uris == null || uris.length == 0) {
				throw new RuntimeException("Edges file is not set in DistributedCache");
			}

			for (int i = 0; i < uris.length; i++) {
				FileSystem fs = FileSystem.get(new Path(centroid_Path).toUri(), context.getConfiguration());
				Path path = new Path(uris[i].toString());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line;
				while ((line = rdr.readLine()) != null) {
					centroids.add(Double.parseDouble(line.split(",")[0]));
				}
			}
		}

		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {
			Double minDist = Double.POSITIVE_INFINITY;
			Double closestCenter = centroids.get(0);
			Integer followerCount = Integer.parseInt(value.toString());
			for (int i = 0; i < centroids.size(); i++) {
				if (Math.abs(centroids.get(i) - followerCount) < minDist) {
					closestCenter = centroids.get(i);
					minDist = Math.abs(centroids.get(i) - followerCount);
				}
			}
			context.write(new Text(closestCenter.toString()), new Text(value.toString()));
		}
	}

	// Reducer - 2 - Centroid Loop
	public static class CentroidReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context)
				throws IOException, InterruptedException {
			Double sum = 0.0;
			int count = 0;
			Double SSE = 0.0;
			Double Centroid = Double.parseDouble(key.toString());
			for (final Text val : values) {
				int followerCount = Integer.parseInt(val.toString());
				count = count + 1;
				sum = sum + followerCount;
				SSE = SSE + Math.abs(Centroid - followerCount);
			}
			Double newCentroid = round((sum / count), 3);
			SSE = round(SSE, 3);
			context.getCounter(New_SSE.new_SSE)
					.setValue(context.getCounter(New_SSE.new_SSE).getValue() + SSE.longValue());
			context.write(new Text(newCentroid.toString()), new Text(SSE.toString()));
		}
	}

	public static Double round(double value, int places) {
		double scale = Math.pow(10, places);
		return Math.round(value * scale) / scale;
	}
}

enum New_SSE {
	new_SSE
}
