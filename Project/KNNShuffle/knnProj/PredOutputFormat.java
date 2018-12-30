package knnProj;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredOutputFormat {
	
	// Final Result - Output formatter for each record
	public static class OutputFormatterMapper extends Mapper<Text, Text, Text, Text> {

		HashMap<Integer, String> testDataMap = new HashMap<Integer, String>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			String testDataPath = context.getConfiguration().get("testDataPath");
			URI[] uris = context.getCacheFiles();
			if (uris == null || uris.length == 0) {
				throw new RuntimeException("Test file is not set in DistributedCache");
			}
			for (int i = 0; i < uris.length; i++) {
				FileSystem fs = FileSystem.get(new Path(testDataPath).toUri(), context.getConfiguration());
				Path path = new Path(uris[i].toString());
				BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line;
				while ((line = rdr.readLine()) != null) {
					String id_record[] = line.split("\t");
					testDataMap.put(Integer.parseInt(id_record[0]), id_record[1]);
				}
			}
		}

		@Override
		public void map(final Text key, final Text value, final Context context)
				throws IOException, InterruptedException {
			String testRecord = testDataMap.get(Integer.parseInt(key.toString()));
			context.write(new Text(testRecord), value);
		}
	}
}
