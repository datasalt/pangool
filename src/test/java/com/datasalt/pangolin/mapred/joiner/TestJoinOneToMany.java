package com.datasalt.pangolin.mapred.joiner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.AfterClass;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.test.PangolinBaseTest;
import com.datasalt.pangolin.mapred.joiner.JoinOneToMany;
import com.datasalt.pangolin.mapred.joiner.JoinOneToMany.ManySideMapper;
import com.datasalt.pangolin.mapred.joiner.JoinOneToMany.OneSideMapper;
import com.datasalt.pangolin.mapred.joiner.JoinOneToMany.OneToManyReducer;
import com.google.common.io.Files;

public class TestJoinOneToMany extends PangolinBaseTest {

	final static String OUTPUT = "output-" + TestJoinOneToMany.class;
	final static String INPUT1  = "input1-" + TestJoinOneToMany.class;
	final static String INPUT2  = "input2-" + TestJoinOneToMany.class;

	static int firstWasSecondClass = 0;
	static boolean noSecondClass = false;
	
	public static class OneMap extends OneSideMapper<LongWritable, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			emit("KeyA", new IntWritable(1));
			emit("KeyB", new IntWritable(2));
			emit("KeyC", new IntWritable(3));
		}
	}
	
	public static class ManyMap extends ManySideMapper<LongWritable, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			emit("KeyA", new Text("foo"));
			emit("KeyA", new Text("bar"));
			emit("KeyB", new Text("oh la la"));
			emit("KeyB", new Text("blah blah"));
			emit("KeyD", new Text("bluu"));
		}
	}	
	
	public static class Reducer extends OneToManyReducer<IntWritable, Text, Text, NullWritable> {
		
		@Override
		protected void onNoOneSideItem(Context ctx) throws IOException, InterruptedException {
		  firstWasSecondClass++;
		}
		
		@Override
		protected void onNoManySideItems(Context ctx) throws IOException, InterruptedException {
			noSecondClass = true;
		}

		@Override
    protected void onPair(IntWritable firstItem, Text secondItem, Context ctx)
        throws IOException, InterruptedException {
			secondItem = (secondItem == null) ? new Text("snull") : secondItem;
			firstItem = (firstItem == null) ? new IntWritable(-1) : firstItem;
			ctx.write(new Text(firstItem + " " + secondItem), NullWritable.get());
    }
	}
	
	protected Job getMultiJoiner(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {		
		JoinOneToMany multiJoiner = new JoinOneToMany("MultiJoiner Test",conf);
		multiJoiner.setReducer(Reducer.class);
		multiJoiner.setOutputKeyClass(Text.class);
		multiJoiner.setOutputValueClass(NullWritable.class);
		multiJoiner.setOutputFormat(TextOutputFormat.class);
		multiJoiner.setOutputPath(new Path(OUTPUT));
		
		Job job = multiJoiner
			.setManySideClass(Text.class)
			.setOneSideClass(IntWritable.class)
			.addManySideInput(new Path(INPUT1), TextInputFormat.class, ManyMap.class)
			.addOneSideInput(new Path(INPUT2), TextInputFormat.class, OneMap.class)
			.getJob();

		return job;
	}
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException {
		File input = new File(INPUT1);
		if(input.exists()) {
			while(!input.delete());
		}
		Files.write("line", input, Charset.defaultCharset());
		input = new File(INPUT2);
		if(input.exists()) {
			while(!input.delete());
		}
		Files.write("line", input, Charset.defaultCharset());

		
		Configuration conf = getConf();	
		Job job = getMultiJoiner(conf);
		job.waitForCompletion(true);
		assertTrue(job.isSuccessful());
		
		File out = new File(OUTPUT, "part-r-00000");
		List<String> lines = Files.readLines(out, Charset.defaultCharset());
		System.out.println(lines);
		
		assertEquals(6, lines.size());
		assertTrue(lines.contains("1 foo"));
		assertTrue(lines.contains("1 bar"));
		assertTrue(lines.contains("2 oh la la"));
		assertTrue(lines.contains("2 blah blah"));
		assertTrue(lines.contains("-1 bluu"));
		assertTrue(lines.contains("3 snull"));

		assertTrue(firstWasSecondClass == 1);
		assertTrue(noSecondClass == true);
		
		cleanUp();
	}
	
	@AfterClass
	public static void cleanUp() throws IOException {
		Configuration conf = new Configuration();
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT));
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(INPUT1));
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(INPUT2));
	}
}
