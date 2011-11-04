package com.datasalt.pangolin.mapred.joiner;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import com.datasalt.pangolin.thrift.test.A;

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
import com.datasalt.pangolin.commons.test.PangolinBaseTest;
import com.datasalt.pangolin.mapred.joiner.MultiJoinChanneledMapper;
import com.datasalt.pangolin.mapred.joiner.MultiJoinDatum;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinReducer;
import com.datasalt.pangolin.mapred.joiner.MultiJoiner;

public class TestMultiJoinerSameClass extends PangolinBaseTest {

	public static final String OUTPUT_FOR_TEST = "test-" +TestMultiJoinerSameClass.class.getName();
	
	/**
	 * Group by ID 
	 * 
	 * @author pere
	 *
	 */
	public static class AMapperSameClass extends MultiJoinChanneledMapper<LongWritable, Text, A> {
		
		protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException, InterruptedException {
			
			A a = new A();
			String[] fields = value.toString().split("\t");
			a.setId(fields[0]);
			a.setUrl(fields[1]);
      emit(a.getId(), a);
		};
	}
	
	private static class TestReducerSameClass extends MultiJoinReducer<Text, Text> {
		
		protected void reduce(@SuppressWarnings("rawtypes") MultiJoinPair arg0, Iterable<MultiJoinDatum<?>> arg1, Context arg2) 
			throws java.io.IOException, InterruptedException {
			
			Iterator<MultiJoinDatum<?>> datums = arg1.iterator();
			MultiJoinDatum<?> datum = datums.next();
      A a = deserialize(datum);
      assertEquals(datum.getChannelId(), 0);
      if(a.getId().equals("id1")) {
	      assertEquals(a.getUrl(), "http://foo.bar");
	      datum = datums.next();
	      assertEquals(datum.getChannelId(), 1);
	      A a2 = deserialize(datum);
	      assertEquals(a2.getUrl(), "http://new.foo.bar");
      } else {
	      assertEquals(a.getUrl(), "http://hello.com");
	      datum = datums.next();
	      assertEquals(datum.getChannelId(), 1);
	      A a2 = deserialize(datum);
	      assertEquals(a2.getUrl(), "http://new.hello.com");	      	
      }
		};
	}
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = getConf();
		MultiJoiner multiJoiner = new MultiJoiner("MultiJoiner Test", conf);
		multiJoiner.setReducer(TestReducerSameClass.class);
		multiJoiner.setOutputKeyClass(Text.class);
		multiJoiner.setOutputValueClass(Text.class);
		multiJoiner.setOutputFormat(TextOutputFormat.class);
		multiJoiner.setOutputPath(new Path(OUTPUT_FOR_TEST));
		Job job = multiJoiner
			.addChanneledInput(0, new Path("src/test/resources/multijoiner.test.a.txt"), A.class, TextInputFormat.class, AMapperSameClass.class)
			.addChanneledInput(1, new Path("src/test/resources/multijoiner.test.same.class.a.txt"), A.class, TextInputFormat.class, AMapperSameClass.class)
			.getJob();
		job.waitForCompletion(true);
		assertTrue(job.isSuccessful());
		
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT_FOR_TEST));
	}
}
