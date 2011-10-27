package com.datasalt.pangolin.mapred.joiner;

import java.io.IOException;

import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangolin.thrift.test.B;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.test.BaseTest;
import com.datasalt.pangolin.mapred.joiner.MultiJoiner;
import com.datasalt.pangolin.mapred.joiner.TestMultiJoiner.AMapper;
import com.datasalt.pangolin.mapred.joiner.TestMultiJoiner.BMapper;
import com.datasalt.pangolin.mapred.joiner.TestMultiJoiner.TestReducer;

public class TestMultiJoinerGlob extends BaseTest {
	
	public static final String OUTPUT_FOR_TEST = "test-" +TestMultiJoinerGlob.class.getName();

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
			.addChanneledInput(0, new Path("resources/glob-folder/*"), A.class, TextInputFormat.class, AMapper.class)
			.addChanneledInput(1, new Path("resources/multijoiner.test.b.txt"), B.class, TextInputFormat.class, BMapper.class)
			.getJob();
		job.waitForCompletion(true);
		
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT_FOR_TEST));
	}
}
