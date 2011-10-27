package com.datasalt.pangolin.mapred.joiner;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangolin.thrift.test.B;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.test.BaseTest;
import com.datasalt.pangolin.mapred.joiner.MultiJoinChanneledMapper;
import com.datasalt.pangolin.mapred.joiner.MultiJoinDatum;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPairText;
import com.datasalt.pangolin.mapred.joiner.MultiJoinReducer;
import com.datasalt.pangolin.mapred.joiner.MultiJoiner;

public class TestMultiJoinerSecondarySort extends BaseTest {
	
	public static final String OUTPUT_FOR_TEST = "test-" +TestMultiJoinerSecondarySort.class.getName();
	
	/**
	 * ID -> URL
	 * 
	 * @author pere
	 *
	 */
	private static class AMapperSecondarySort extends MultiJoinChanneledMapper<LongWritable, Text, A> {
		
		protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException, InterruptedException {
			
			A a = new A();
			String[] fields = value.toString().split("\t");
			a.setId(fields[0]);
			a.setUrl(fields[1]);
			String ss = fields[2];
      emit(a.getUrl(), new Text(ss), a);
		};
	}
	
	public static class BMapperSecondarySort extends MultiJoinChanneledMapper<LongWritable, Text, B> {
		
		protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException, InterruptedException {
			
			B b = new B();
			String[] fields = value.toString().split("\t");
			b.setUrl(fields[0]);
			b.setUrlNorm(fields[1]);
			emit(b.getUrl(), new Text(""), b);
		};
	}
	
	private static class TestReducerSecondarySort extends MultiJoinReducer<Text, Text> {
		
		protected void reduce(@SuppressWarnings("rawtypes") MultiJoinPair arg0, Iterable<MultiJoinDatum<?>> arg1, Context arg2) 
			throws java.io.IOException, InterruptedException {
			
			Iterator<MultiJoinDatum<?>> datums = arg1.iterator();
			MultiJoinDatum<?> datum = datums.next();
      A a = deserialize(datum);
      assertEquals(a.getId(), "id2");
      datum = datums.next();
      A a2 = deserialize(datum);
      datum = datums.next();
      assertEquals(a2.getId(), "id1");	      
      B b = deserialize(datum);
      arg2.write(new Text(a.getUrl()), new Text(b.getUrlNorm()));
		};
	}
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = getConf();
		MultiJoiner multiJoiner = new MultiJoiner("MultiJoiner Test",conf);
		multiJoiner.setReducer(TestReducerSecondarySort.class);
		multiJoiner.setOutputKeyClass(Text.class);
		multiJoiner.setOutputValueClass(Text.class);
		multiJoiner.setOutputFormat(TextOutputFormat.class);
		multiJoiner.setOutputPath(new Path(OUTPUT_FOR_TEST));
		Job job = multiJoiner
			.setMultiJoinPairClass(MultiJoinPairText.class)
			.addChanneledInput(0, new Path("resources/multijoiner.test.a.2.txt"), A.class, TextInputFormat.class, AMapperSecondarySort.class)
			.addChanneledInput(1, new Path("resources/multijoiner.test.b.2.txt"), B.class, TextInputFormat.class, BMapperSecondarySort.class)
			.getJob();
		job.waitForCompletion(true);
		if(!job.isSuccessful()) {
			throw new RuntimeException("Unit test failed");
		}
		
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT_FOR_TEST));
	}
}
