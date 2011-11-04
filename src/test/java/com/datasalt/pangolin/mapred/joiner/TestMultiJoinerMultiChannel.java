package com.datasalt.pangolin.mapred.joiner;

import static org.junit.Assert.assertTrue;

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
import com.datasalt.pangolin.mapred.joiner.MultiJoinDatum;
import com.datasalt.pangolin.mapred.joiner.MultiJoinMultiChannelMapper;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinReducer;
import com.datasalt.pangolin.mapred.joiner.MultiJoiner;

/**
 * Unit test for the {@link MultiJoinMultiChannelMapper} to assert that we can emit different channels from the same mapper.
 * 
 * @author pere
 *
 */
public class TestMultiJoinerMultiChannel extends PangolinBaseTest {

	public static final String OUTPUT_FOR_TEST = "test-" + TestMultiJoinerMultiChannel.class.getName();

	/**
	 * This mapper emits first an instance of A and then an instance of B with the same data.
	 * 
	 * @author pere
	 *
	 */
	public static class ABMapper extends MultiJoinMultiChannelMapper<LongWritable, Text> {

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {

			/*
			 * Emit A first ( channel = 0 )
			 */
			A a = new A();
			String[] fields = value.toString().split("\t");
			a.setId(fields[0]);
			a.setUrl(fields[1]);
			emit(a.getId(), a, 0);

			/*
			 * Then B ( channel = 1 )
			 */
			B b = new B();
			b.setUrl(fields[0]);
			b.setUrlNorm(fields[1]);
			emit(b.getUrl(), b, 1);
		};
	}

	public static class TestReducer extends MultiJoinReducer<Text, Text> {

		@Override
		protected void reduce(@SuppressWarnings("rawtypes") MultiJoinPair arg0, Iterable<MultiJoinDatum<?>> arg1,
		    Context arg2) throws java.io.IOException, InterruptedException {

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
		MultiJoiner multiJoiner = new MultiJoiner("MultiJoiner Test", conf);
		multiJoiner.setReducer(TestReducer.class);
		multiJoiner.setOutputKeyClass(Text.class);
		multiJoiner.setOutputValueClass(Text.class);
		multiJoiner.setOutputFormat(TextOutputFormat.class);
		multiJoiner.setOutputPath(new Path(OUTPUT_FOR_TEST));

		Job job = multiJoiner
				.addInput(new Path("src/test/resources/multijoiner.test.a.txt"), TextInputFormat.class, ABMapper.class)
		    .setChannelDatumClass(0, A.class)
		    .setChannelDatumClass(1, B.class)
		    .getJob();
		
		job.waitForCompletion(true);
		assertTrue(job.isSuccessful());

		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT_FOR_TEST));
	}
}
