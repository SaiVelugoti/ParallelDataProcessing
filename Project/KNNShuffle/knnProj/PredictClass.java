package knnProj;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import knnProj.HelperClasses.DistClass;
import knnProj.HelperClasses.TestData;

// Calculate distance between each test record and each training record

public class PredictClass {

	public static class KNN_Multi_Mapper extends Mapper<Object, Text, Text, DistClass> {

		// Storing all test records
		ArrayList<TestData> testDataList = new ArrayList<TestData>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			String testDataPath = context.getConfiguration().get("testDataPath");
			URI[] uris = context.getCacheFiles();
			if (uris == null || uris.length == 0) {
				throw new RuntimeException("Test file is not set in DistributedCache");
			}
			// Integer recordNum = 1;
			for (int i = 0; i < uris.length; i++) {
				FileSystem fs = FileSystem.get(new Path(testDataPath).toUri(), context.getConfiguration());
				Path path = new Path(uris[i].toString());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line;
				while ((line = rdr.readLine()) != null) {
					String id_record[] = line.split("\t");
					String[] tokens = id_record[1].split(",");
					TestData testData = new TestData();
					testData.set(Integer.parseInt(id_record[0]), tokens[1], Double.parseDouble(tokens[2]), tokens[3]);
					testDataList.add(testData);
				}
			}
		}

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split(",");
			String train_type = tokens[1];
			Double train_amount = Double.parseDouble(tokens[2]);
			String train_user = tokens[3];
			String train_class = tokens[9];

			Double dist_type;
			Double dist_amount;
			Double dist_user;

			for (TestData td : testDataList) {
				dist_type = distance(train_type, td.getTestType());
				dist_amount = dist_amount(td.getTestAmount(), train_amount);
				dist_user = distance(train_user, td.getTestUser());
				Double distance = dist_type + dist_amount + dist_user;
				DistClass distanceClassifier = new DistClass();
				distanceClassifier.set(distance, train_class);
				context.write(new Text(td.getTestIdentifier()), distanceClassifier);
			}
		}

		Double distance(String s1, String s2) {
			if (s1.equals(s2))
				return 0.0;
			else
				return 1.0;
		}

		Double dist_amount(Double d1, Double d2) {
			return Math.abs(d1 - d2);
		}
	}

	public static class KNN_Multi_Reducer extends Reducer<Text, DistClass, Text, Text> {

		Integer k;
		TreeMap<Double, String> KNN_values = new TreeMap<Double, String>();

		@Override
		protected void setup(Context context) {
			k = Integer.parseInt(context.getConfiguration().get("k"));
		}

		@Override
		public void reduce(Text key, Iterable<DistClass> values, Context context)
				throws IOException, InterruptedException {

			for (Object ds : values) {
				if (ds instanceof DistClass) {
					KNN_values.put(((DistClass) ds).getDistance(), ((DistClass) ds).getClassifier());
					if (KNN_values.size() > k)
						KNN_values.pollLastEntry();
				}
			}

			List<String> classiferValues = new ArrayList<String>(KNN_values.values());
			Map<String, Integer> classifierFreq = new HashMap<String, Integer>();

			for (int i = 0; i < KNN_values.size(); i++) {
				Integer freq = classifierFreq.get(classiferValues.get(i));
				if (freq == null)
					classifierFreq.put(classiferValues.get(i), 1);
				else
					classifierFreq.put(classiferValues.get(i), freq + 1);
			}

			String possibleClassifier = null;
			int maxFrequency = -1;
			for (Map.Entry<String, Integer> entry : classifierFreq.entrySet()) {
				if (entry.getValue() > maxFrequency) {
					possibleClassifier = entry.getKey();
					maxFrequency = entry.getValue();
				}
			}
			context.write(new Text(key), new Text(possibleClassifier));
		}
	}
}
