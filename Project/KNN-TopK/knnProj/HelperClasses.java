package knnProj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HelperClasses {

	public static class DistClass implements WritableComparable<DistClass> {

		private Double distance = 0.0;
		private String classifier = null;

		public void set(Double dist, String s) {
			distance = dist;
			classifier = s;
		}

		public Double getDistance() {
			return distance;
		}

		public String getClassifier() {
			return classifier;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			distance = in.readDouble();
			classifier = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(distance);
			out.writeUTF(classifier);
		}

		@Override
		public int compareTo(DistClass dc) {
			return this.classifier.compareTo(dc.getClassifier());
		}
	}

	public static class TestData implements WritableComparable<TestData> {
		private String test_identifier;
		private String test_type;
		private Double test_amount;
		private String test_user;

		public void set(Integer identifier, String type, Double amount, String user) {
			test_identifier = identifier.toString();
			test_type = type;
			test_amount = amount;
			test_user = user;
		}

		public String getTestIdentifier() {
			return test_identifier;
		}

		public String getTestType() {
			return test_type;
		}

		public Double getTestAmount() {
			return test_amount;
		}

		public String getTestUser() {
			return test_user;
		}

		@Override
		public String toString() {
			return test_type + "," + test_amount.toString() + "," + test_user;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			test_identifier = in.readUTF();
			test_type = in.readUTF();
			test_amount = in.readDouble();
			test_user = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(test_identifier);
			out.writeUTF(test_type);
			out.writeDouble(test_amount);
			out.writeUTF(test_user);
		}

		@Override
		public int compareTo(TestData td) {
			return this.test_identifier.compareTo(td.getTestIdentifier());
		}
	}
}
