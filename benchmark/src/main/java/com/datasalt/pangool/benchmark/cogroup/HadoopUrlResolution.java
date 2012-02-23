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
package com.datasalt.pangool.benchmark.cogroup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Code for solving the URL Resolution CoGroup Problem in Hadoop Java Map/Red API.
 * <p>
 * The URL Resolution CoGroup Problem is: We have one file with URL Registers: {url timestamp ip} and another file with
 * canonical URL mapping: {url cannonicalUrl}. We want to obtain the URL Registers file with the url substituted with
 * the canonical one according to the mapping file: {canonicalUrl timestamp ip}.
 */
public class HadoopUrlResolution {

	public final static int SOURCE_URL_MAP = 0;
	public final static int SOURCE_URL_REGISTER = 1;

	public static class UrlRegJoinUrlMap implements WritableComparable<UrlRegJoinUrlMap> {

		// --- Common fields --- //
		private Text groupUrl = new Text();
		private int sourceId;

		// --- Url register --- //
		private Text ip = new Text();
		private long timestamp;

		// --- Url map --- //
		private Text cannonicalUrl = new Text();

		public void setUrlRegister(String groupUrl, String ip, long timestamp) {
			this.groupUrl.set(groupUrl);
			sourceId = SOURCE_URL_REGISTER;
			this.ip.set(ip);
			this.timestamp = timestamp;
		}

		public void setUrlMap(String groupUrl, String cannonicalUrl) {
			this.groupUrl.set(groupUrl);
			sourceId = SOURCE_URL_MAP;
			this.cannonicalUrl.set(cannonicalUrl);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			groupUrl.readFields(in);
			sourceId = (int) in.readByte();
			if(sourceId == SOURCE_URL_MAP) {
				cannonicalUrl.readFields(in);
			} else {
				ip.readFields(in);
				timestamp = in.readLong();
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			groupUrl.write(out);
			out.writeByte((byte) sourceId);
			if(sourceId == SOURCE_URL_MAP) {
				cannonicalUrl.write(out);
			} else {
				ip.write(out);
				out.writeLong(timestamp);
			}
		}

		@Override
		public int hashCode() {
			if(sourceId == SOURCE_URL_MAP) {
				return ((groupUrl.hashCode() * 31) + cannonicalUrl.hashCode()) * 31;
			} else {
				return ((((groupUrl.hashCode() * 31) + ip.hashCode()) * 31) + (int) timestamp) * 31;
			}
		}

		@Override
		public boolean equals(Object right) {
			if(right instanceof UrlRegJoinUrlMap) {
				UrlRegJoinUrlMap r = (UrlRegJoinUrlMap) right;
				if(sourceId == r.sourceId) {
					if(groupUrl.equals(r.groupUrl)) {
						if(sourceId == SOURCE_URL_MAP) {
							return cannonicalUrl.equals(r.cannonicalUrl);
						} else {
							return ip.equals(r.ip) && timestamp == r.timestamp;
						}
					}
				}
			}
			return false;
		}

		public static class Comparator extends WritableComparator {
			public Comparator() {
				super(UrlRegJoinUrlMap.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
				try {
					int offset1 = s1;
					int offset2 = s2;
					// Group url
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
					// Source id
					int sourceId1 = (int) b1[offset1];
					int sourceId2 = (int) b2[offset2];
					if(sourceId1 != sourceId2) {
						return sourceId1 > sourceId2 ? 1 : -1;
					}
					return 0;
				} catch(IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		static { // register this comparator
			WritableComparator.define(UrlRegJoinUrlMap.class, new Comparator());
		}

		@Override
		public int compareTo(UrlRegJoinUrlMap o) {
			int cmp = groupUrl.compareTo(o.groupUrl);
			if(cmp != 0) {
				return cmp;
			}
			if(sourceId != o.sourceId) {
				return sourceId > o.sourceId ? 1 : -1;
			}
			return 0;
		}

		public String toString() {
			if(sourceId == SOURCE_URL_MAP) {
				return groupUrl.toString() + "," + cannonicalUrl.toString();
			} else {
				return groupUrl.toString() + "," + ip.toString() + "," + timestamp;
			}
		}
	}

	public static class KeyPartitioner implements Partitioner<UrlRegJoinUrlMap, NullWritable> {
		@Override
		public int getPartition(UrlRegJoinUrlMap key, NullWritable value, int numPartitions) {
			return Math.abs(key.groupUrl.hashCode()) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0) {
		}
	}

	public static class GroupingComparator implements RawComparator<UrlRegJoinUrlMap> {

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int offset1 = s1;
				int offset2 = s2;
				int strSize1 = WritableComparator.readVInt(b1, offset1);
				int strSize2 = WritableComparator.readVInt(b2, offset2);
				offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
				offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
				return WritableComparator.compareBytes(b1, offset1, strSize1, b2, offset2, strSize2);
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public int compare(UrlRegJoinUrlMap o1, UrlRegJoinUrlMap o2) {
			return o1.groupUrl.compareTo(o2.groupUrl);
		}
	}

	/**
	 * 
	 */
	public static class UrlMapClass implements Mapper<LongWritable, Text, UrlRegJoinUrlMap, NullWritable> {

		private final UrlRegJoinUrlMap key = new UrlRegJoinUrlMap();

		@Override
		public void configure(JobConf arg0) {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void map(LongWritable ignore, Text inValue, OutputCollector<UrlRegJoinUrlMap, NullWritable> context,
		    Reporter arg3) throws IOException {
			String[] fields = inValue.toString().split("\t");
			key.setUrlMap(fields[0], fields[1]);
			context.collect(key, NullWritable.get());
		}
	}

	/**
	 * 
	 */
	public static class UrlRegisterMapClass implements Mapper<LongWritable, Text, UrlRegJoinUrlMap, NullWritable> {

		private final UrlRegJoinUrlMap key = new UrlRegJoinUrlMap();

		@Override
		public void configure(JobConf arg0) {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void map(LongWritable ignore, Text inValue, OutputCollector<UrlRegJoinUrlMap, NullWritable> context,
		    Reporter arg3) throws IOException {

			String[] fields = inValue.toString().split("\t");
			key.setUrlRegister(fields[0], fields[2], Long.parseLong(fields[1]));
			context.collect(key, NullWritable.get());
		}
	}

	public static class Reduce extends Reducer<UrlRegJoinUrlMap, NullWritable, Text, NullWritable> {

		Text result = new Text();

		protected void reduce(UrlRegJoinUrlMap key, Iterable<NullWritable> values, Context ctx) throws IOException,
		    InterruptedException {
			String cannonicalUrl = null;
			for(@SuppressWarnings("unused")
			NullWritable value : values) {
				if(key.sourceId == SOURCE_URL_MAP) {
					cannonicalUrl = key.cannonicalUrl.toString();
				} else {
					result.set(cannonicalUrl + "\t" + key.timestamp + "\t" + key.ip);
					ctx.write(result, NullWritable.get());
				}
			}
		};
	}

	public final static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3) {
			System.err.println("Usage: urlresolution <url-map> <url-register> <out>");
			System.exit(2);
		}
		JobConf job = new JobConf(conf);
		FileSystem fS = FileSystem.get(conf);
		fS.delete(new Path(otherArgs[2]), true);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, UrlMapClass.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, UrlRegisterMapClass.class);

		job.setJarByClass(HadoopUrlResolution.class);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setOutputValueGroupingComparator(GroupingComparator.class);

		job.setMapOutputKeyClass(UrlRegJoinUrlMap.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		Job j = new Job(job);
		j.setReducerClass(Reduce.class);
		j.waitForCompletion(true);
	}
}
