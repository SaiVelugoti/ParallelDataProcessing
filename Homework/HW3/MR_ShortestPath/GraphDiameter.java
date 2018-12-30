package sp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

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

public class GraphDiameter extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(GraphDiameter.class);
	private static int sourceVertex = 0;

	// Adjacency List Mapper
	public static class AdjListMapper extends Mapper<Text, Text, Text, Text> {
		

		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	// Adjacency List Reducer
	public static class AdjListReducer extends Reducer<Text, Text, Object, Object> {

		private String outString = "";

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer str = new StringBuffer();
			for (Text s : values) {
				str.append(s.toString() + " ");
			}
			if (Integer.parseInt(key.toString()) == sourceVertex) {
				outString = key.toString() + "|" + str.toString().trim() + "|" + "T" + "|" + 0;
			} else {
				outString = key.toString() + "|" + str.toString().trim() + "|" + "F" + "|" + Integer.MAX_VALUE;
			}
			// format of outString(4 values) :
			// key | adjList separated by spaces | T/F source code | distance
			context.write(key, outString);
		}
	}

	// Shortest Path Mapper
	public static class SPMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {

			context.write(key, value);

			String tokens[] = value.toString().split("\\|");
			if (tokens.length > 1) {

				if (tokens[2].equals("T")) {
					Integer d = Integer.parseInt(tokens[3]) + 1;
					for (String vertex : tokens[1].split(" ")) {
						context.write(new Text(vertex), new Text(d.toString()));
					}
				}
			}
		}
	}

	// Shortest Path Reducer
	public static class SPReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Integer minDist = Integer.MAX_VALUE;
			String M = "";

			for (Text s : values) {
				String s1 = s.toString();
				if (s1.split("\\|").length > 1)
					M = s1;
				else if (Integer.parseInt(s1) < minDist)
					minDist = Integer.parseInt(s1);
			}

			String tokens[] = M.split("\\|");
			if (!M.isEmpty() && M != "" && tokens.length > 1) {
				if (minDist < Integer.parseInt(tokens[3])) {
					context.getCounter(PathCounter.Counter).increment(1);
					tokens[3] = minDist.toString();
					tokens[2] = "T";
				}
			}
			M = Arrays.stream(tokens).collect(Collectors.joining("|"));
			if (M.isEmpty()) {
				M = key.toString() + "|" + " | T |" + minDist.toString();
			}
			context.write(key, new Text(M));
		}
	}

	// Diameter Mapper
	public static class DiameterMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {

			String tokens[] = value.toString().split("\\|");
			if (tokens.length > 1) {
				if (Integer.parseInt(tokens[3]) < Integer.MAX_VALUE) {
					context.write(new Text("dist"), new Text(tokens[3]));
				}
			}
		}
	}

	// Diameter Reducer
	public static class DiameterReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Integer diameter = Integer.MIN_VALUE;

			for (Text val : values) {
				if (Integer.parseInt(val.toString()) > diameter) {
					diameter = Integer.parseInt(val.toString());
				}
			}
			context.write(new Text("Diameter"), new Text(diameter.toString()));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

//		// Delete output directory, only to ease local development; will not
//		// work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path("adjacencyListFile"))) {
//			fileSystem.delete(new Path("adjacencyListFile"), true);
//		}
//		if (fileSystem.exists(new Path("iterationOutput"))) {
//			fileSystem.delete(new Path("iterationOutput"), true);
//		}
//		if (fileSystem.exists(new Path("shortestPath"))) {
//			fileSystem.delete(new Path("shortestPath"), true);
//		}
//		if (fileSystem.exists(new Path("GraphDiameter"))) {
//			fileSystem.delete(new Path("GraphDiameter"), true);
//		}
//		
		// ---------Job 1

		final Job job_AdjList = Job.getInstance(conf, "Adjacency List");
		job_AdjList.setJarByClass(GraphDiameter.class);

		final Configuration jobConf = job_AdjList.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job_AdjList.setInputFormatClass(KeyValueTextInputFormat.class);
		job_AdjList.setMapperClass(AdjListMapper.class);
		job_AdjList.setReducerClass(AdjListReducer.class);

		job_AdjList.setOutputKeyClass(Text.class);
		job_AdjList.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job_AdjList, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_AdjList, new Path(args[1]));

		if (job_AdjList.waitForCompletion(true) == false) {
			System.exit(1);
		}

		// ---------Job 2
		int iterationCount = 1;
		String iterationOutput = args[2]+"_" + iterationCount;
		String iterationInput = args[1];
	
		boolean iterate = true;

		while (iterate) {

			final Job job_1 = Job.getInstance(conf, "ShortestPath");
			job_1.setJarByClass(GraphDiameter.class);

			final Configuration jobConf_1 = job_1.getConfiguration();
			jobConf_1.set("mapreduce.output.textoutputformat.separator", ",");

			job_1.setInputFormatClass(KeyValueTextInputFormat.class);
			job_1.setMapperClass(SPMapper.class);
			job_1.setReducerClass(SPReducer.class);

			job_1.setOutputKeyClass(Text.class);
			job_1.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job_1, new Path(iterationInput));
			FileOutputFormat.setOutputPath(job_1, new Path(iterationOutput));

			if (job_1.waitForCompletion(true)) {
				long counter = job_1.getCounters().findCounter(PathCounter.Counter).getValue();
				if (counter > 0){
					iterate = true;
					job_1.getCounters().findCounter(PathCounter.Counter).setValue(0);
					iterationCount = iterationCount+1;
					iterationInput = iterationOutput;
					iterationOutput = args[2]+"_" + iterationCount;
				}
				else
					iterate = false;
			}
			
		}

		// ---------Job 3
		final Job job_Diameter = Job.getInstance(conf, "Graph Diameter");
		job_Diameter.setJarByClass(GraphDiameter.class);

		final Configuration jobConf_3 = job_Diameter.getConfiguration();
		jobConf_3.set("mapreduce.output.textoutputformat.separator", ",");

		job_Diameter.setInputFormatClass(KeyValueTextInputFormat.class);
		job_Diameter.setMapperClass(DiameterMapper.class);
		job_Diameter.setReducerClass(DiameterReducer.class);

		job_Diameter.setOutputKeyClass(Text.class);
		job_Diameter.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job_Diameter, new Path(iterationOutput));
		FileOutputFormat.setOutputPath(job_Diameter, new Path(args[3]));

		return job_Diameter.waitForCompletion(true)? 0:1;
	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <adjacencyList> <ShortestPath> <output-dir>");
		}

		// int k = Integer.parseInt(args[2]);
		// int min = Integer.parseInt(args[3]);
		// int max = Integer.parseInt(args[4]);
		// int sampledUser = (new Random()).nextInt(max - min + 1) + min;

		int sampledUser = 150;
		ArrayList<Integer> randomSample = new ArrayList<Integer>();
		randomSample.add(sampledUser);
		sourceVertex = randomSample.get(0);
		try {
			ToolRunner.run(new GraphDiameter(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}

enum PathCounter {
	Counter
}
