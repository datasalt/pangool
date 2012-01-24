package com.datasalt.pangool.io;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.BaseCoGrouperTest;
import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Sorting;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.IdentityGroupHandler;
import com.datasalt.pangool.api.IdentityInputProcessor;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;
import com.google.common.io.Files;

public class TestTupleInputOutputFormat extends BaseCoGrouperTest {

	public static String OUT = TestTupleInputOutputFormat.class.getName() + "-out";
	public static String OUT_TEXT = TestTupleInputOutputFormat.class.getName() + "-out-text";
	public static String IN = TestTupleInputOutputFormat.class.getName() + "-in";

	public static class MyInputProcessor extends InputProcessor<LongWritable, Text> {

		Tuple tuple = new Tuple();

		@Override
		public void process(LongWritable key, Text value, Collector collector) throws IOException, InterruptedException {

			tuple.setString("title", "title");
			tuple.setString("content", value.toString());
			collector.write(tuple);
		}
	}

	public static class MyGroupHandler extends GroupHandler<Text, Text> {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, State state,
		    ReduceContext<ITuple, NullWritable, Text, Text> context) throws IOException, InterruptedException,
		    CoGrouperException {

			for(ITuple tuple: tuples) {
				context.write(new Text(tuple.getString("title")), new Text(tuple.getString("content")));
			}
		}
	}
	
	@Test
	public void test() throws InvalidFieldException, CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {
		
		Files.write("foo1 bar1\nbar2 foo2", new File(IN), Charset.forName("UTF-8"));
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		Path outPathText = new Path(OUT_TEXT);
		HadoopUtils.deleteIfExists(fS, outPath);
		HadoopUtils.deleteIfExists(fS, outPathText);
		
		Schema schema = Schema.parse("title:string, content:string");
		PangoolConfigBuilder configBuilder = new PangoolConfigBuilder();
		configBuilder.addSchema(0, schema);
		configBuilder.setGroupByFields("title");
		configBuilder.setSorting(Sorting.parse("title asc, content asc"));

		CoGrouper coGrouper = new CoGrouper(configBuilder.build(), conf);
		coGrouper.setGroupHandler(IdentityGroupHandler.class);
		coGrouper.setTupleOutput(outPath, schema); // setTupleOutput method
		coGrouper.addInput(inPath, TextInputFormat.class, MyInputProcessor.class);

		coGrouper.createJob().waitForCompletion(true);
		
		// Use output as input of new CoGrouper
		
		coGrouper = new CoGrouper(configBuilder.build(), conf);
		coGrouper.setGroupHandler(MyGroupHandler.class);
		coGrouper.setOutput(outPathText, TextOutputFormat.class, Text.class, Text.class);
		coGrouper.addTupleInput(outPath, IdentityInputProcessor.class); // addTupleInput method
		coGrouper.createJob().waitForCompletion(true);
		
		Assert.assertEquals("title\tbar2 foo2\ntitle\tfoo1 bar1",
				Files.toString(new File(OUT_TEXT + "/" + "part-r-00000"), Charset.forName("UTF-8")).trim());
		
		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
		HadoopUtils.deleteIfExists(fS, outPathText);
	}
}
