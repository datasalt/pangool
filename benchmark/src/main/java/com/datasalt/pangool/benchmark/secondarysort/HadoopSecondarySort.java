/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.benchmark.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Code for solving a secondary sort problem with Hadoop Java Map/Red API.
 * <p>
 * The secondary sort problem is: We have a file with sales registers: {departmentId nameId timestamp saleValue}. We
 * want to obtain meaningful statistics grouping by all people who perform sales (departmentId+nameId). We want to
 * obtain total sales value for certain periods of time, therefore we need to registers in each group to come sorted by
 * "timestamp".
 */
public class HadoopSecondarySort {

	public static class ComplexType implements WritableComparable<ComplexType> {

		private int intField;
		private Text strField = new Text();
		private long longField;

		public void set(int intField, String strField, long longField) {
			this.intField = intField;
			this.strField.set(strField);
			this.longField = longField;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			intField = in.readInt();
			strField.readFields(in);
			longField = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(intField);
			strField.write(out);
			out.writeLong(longField);
		}

		@Override
		public int hashCode() {
			return (intField * 31 + strField.hashCode()) * 31 + (int) longField;
		}

		@Override
		public boolean equals(Object right) {
			if(right instanceof ComplexType) {
				ComplexType r = (ComplexType) right;
				return r.intField == intField && r.strField.equals(strField) && r.longField == longField;
			} else {
				return false;
			}
		}

		public static class Comparator extends WritableComparator {
			public Comparator() {
				super(ComplexType.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
				try {
					int val1 = WritableComparator.readInt(b1, s1);
					int val2 = WritableComparator.readInt(b2, s2);
					if(val1 != val2) {
						return val1 > val2 ? 1 : -1;
					}
					int offset1 = s1 + 4;
					int offset2 = s2 + 4;
					int strSize1 = WritableComparator.readVInt(b1, offset1);
					int strSize2 = WritableComparator.readVInt(b2, offset2);
					offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
					offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
					int cmp = WritableComparator.compareBytes(b1, offset1, strSize1, b2, offset2, strSize2);
					if(cmp != 0) {
						return cmp;
					}
					offset1 += strSize1;
					offset2 += strSize2;
					long f1 = WritableComparator.readLong(b1, offset1);
					long f2 = WritableComparator.readLong(b2, offset2);
					if(f1 != f2) {
						return f1 > f2 ? 1 : -1;
					}
					return 0;
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		static { // register this comparator
			WritableComparator.define(ComplexType.class, new Comparator());
		}

		@Override
		public int compareTo(ComplexType o) {
			if(intField != o.intField) {
				return intField > o.intField ? 1 : -1;
			}
			int cmp = strField.compareTo(o.strField);
			if(cmp != 0) {
				return cmp;
			}
			if(longField != o.longField) {
				return longField > o.longField ? 1 : -1;
			}
			return 0;
		}

		public String toString() {
			return intField + "," + strField + "," + longField;
		}
	}

	public static class KeyPartitioner extends Partitioner<ComplexType, DoubleWritable> {
		@Override
		public int getPartition(ComplexType key, DoubleWritable value, int numPartitions) {
			return Math.abs(key.intField * 127 + key.strField.hashCode() * 127) % numPartitions;
		}
	}

	public static class GroupingComparator implements RawComparator<ComplexType> {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int val1 = WritableComparator.readInt(b1, s1);
				int val2 = WritableComparator.readInt(b2, s2);
				if(val1 != val2) {
					return val1 > val2 ? 1 : -1;
				}
				int offset1 = s1 + 4;
				int offset2 = s2 + 4;
				int strSize1 = WritableComparator.readVInt(b1, offset1);
				int strSize2 = WritableComparator.readVInt(b2, offset2);
				offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
				offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
				int cmp = WritableComparator.compareBytes(b1, offset1, strSize1, b2, offset2, strSize2);
				return cmp;
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public int compare(ComplexType o1, ComplexType o2) {
			if(o1.intField != o2.intField) {
				return o1.intField > o2.intField ? 1 : -1;
			}
			return o1.strField.compareTo(o2.strField);
		}
	}

	/**
	 * 
	 */
	public static class MapClass extends Mapper<LongWritable, Text, ComplexType, DoubleWritable> {

		private final ComplexType key = new ComplexType();
		private final DoubleWritable doubleWritable = new DoubleWritable();

		@Override
		public void map(LongWritable inKey, Text inValue, Context context) throws IOException, InterruptedException {
			String[] fields = inValue.toString().split("\t");
			key.set(Integer.parseInt(fields[0]), fields[1], Long.parseLong(fields[2]));
			doubleWritable.set(Double.parseDouble(fields[3]));
			context.write(key, doubleWritable);
		}
	}

	/**
	 * 
	 */
	public static class Reduce extends Reducer<ComplexType, DoubleWritable, Text, NullWritable> {

		Text result = new Text();

		@Override
		public void reduce(ComplexType key, Iterable<DoubleWritable> values, Context context) throws IOException,
		    InterruptedException {

			String group = key.intField + "\t" + key.strField;
			double accumPayments = 0;
			for(DoubleWritable value : values) {
				accumPayments += value.get();
			}
			result.set(group + "\t" + accumPayments);
			context.write(result, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("Usage: secondarysrot <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Hadoop Secondary Sort");
		FileSystem fS = FileSystem.get(conf);
		fS.delete(new Path(otherArgs[1]), true);

		job.setJarByClass(HadoopSecondarySort.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);

		job.setMapOutputKeyClass(ComplexType.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);
	}
}