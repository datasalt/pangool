package com.datasalt.pangolin.mapred.joiner;

import static org.junit.Assert.*;

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
import com.datasalt.pangolin.commons.test.PangolinBaseTest;
import com.datasalt.pangolin.mapred.joiner.MultiJoinChanneledMapper;
import com.datasalt.pangolin.mapred.joiner.MultiJoinDatum;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinReducer;
import com.datasalt.pangolin.mapred.joiner.MultiJoiner;

public class TestMultiJoiner extends PangolinBaseTest {
	
	public static final String OUTPUT_FOR_TEST = "test-" +TestMultiJoiner.class.getName();

	/**
	 * ID -> URL
	 * 
	 * @author pere
	 *
	 */
	public static class AMapper extends MultiJoinChanneledMapper<LongWritable, Text, A> {
		
		protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException, InterruptedException {
			
			A a = new A();
			String[] fields = value.toString().split("\t");
			a.setId(fields[0]);
			a.setUrl(fields[1]);
      emit(a.getUrl(), a);
		};
	}
	
	/**
	 * URL -> URL_NORM
	 * 
	 * @author pere
	 *
	 */
	public static class BMapper extends MultiJoinChanneledMapper<LongWritable, Text, B> {
		
		protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException, InterruptedException {
			
			B b = new B();
			String[] fields = value.toString().split("\t");
			b.setUrl(fields[0]);
			b.setUrlNorm(fields[1]);
      emit(b.getUrl(), b);
		};
	}

	public static class TestReducer extends MultiJoinReducer<Text, Text> {
		
		@Override
		protected void reduce(@SuppressWarnings("rawtypes") MultiJoinPair arg0, Iterable<MultiJoinDatum<?>> arg1, Context arg2) 
			throws java.io.IOException, InterruptedException {
			
			Iterator<MultiJoinDatum<?>> datums = arg1.iterator();
			/*
			 * We must receive exactly two datums and the order must be A, B
			 */
			MultiJoinDatum<?> datum = datums.next();
      A a = deserialize(datum);
      datum = datums.next();
      B b = deserialize(datum);
      arg2.write(new Text(a.getUrl()), new Text(b.getUrlNorm()));
		};
	}
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = getConf();
		MultiJoiner multiJoiner = new MultiJoiner("MultiJoiner Test",conf);
		multiJoiner.setReducer(TestReducer.class);
		multiJoiner.setOutputKeyClass(Text.class);
		multiJoiner.setOutputValueClass(Text.class);
		multiJoiner.setOutputFormat(TextOutputFormat.class);
		multiJoiner.setOutputPath(new Path(OUTPUT_FOR_TEST));
		
		
		Job job = multiJoiner
			.addChanneledInput(2, new Path("src/test/resources/multijoiner.test.a.txt"), A.class, TextInputFormat.class, AMapper.class)
			.addChanneledInput(4, new Path("src/test/resources/multijoiner.test.b.txt"), B.class, TextInputFormat.class, BMapper.class)
			.getJob();
		job.waitForCompletion(true);
		assertTrue(job.isSuccessful());
		
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT_FOR_TEST));
	}
}
