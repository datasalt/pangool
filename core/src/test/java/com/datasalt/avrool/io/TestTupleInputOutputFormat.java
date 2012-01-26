package com.datasalt.avrool.io;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.avrool.BaseCoGrouperTest;
import com.datasalt.avrool.CoGrouper;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Schema;
import com.datasalt.avrool.Sorting;
import com.datasalt.avrool.api.GroupHandler;
import com.datasalt.avrool.api.IdentityGroupHandler;
import com.datasalt.avrool.api.IdentityInputProcessor;
import com.datasalt.avrool.api.InputProcessor;
import com.datasalt.avrool.commons.HadoopUtils;
import com.datasalt.avrool.io.tuple.ITuple;
import com.datasalt.avrool.io.tuple.Tuple;
import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;
import com.google.common.io.Files;

public class TestTupleInputOutputFormat extends BaseCoGrouperTest {

	public static String OUT = TestTupleInputOutputFormat.class.getName() + "-out";
	public static String OUT_TEXT = TestTupleInputOutputFormat.class.getName() + "-out-text";
	public static String IN = TestTupleInputOutputFormat.class.getName() + "-in";

	public static class MyInputProcessor extends InputProcessor<LongWritable, Text> {

		Tuple tuple = new Tuple();

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector) throws IOException, InterruptedException {

			tuple.setString("title", "title");
			tuple.setString("content", value.toString());
			collector.write(tuple);
		}
	}

	public static class MyGroupHandler extends GroupHandler<Text, Text> {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext<Text, Text> context,
		    Collector<Text, Text> collector) throws IOException, InterruptedException, CoGrouperException {

			for(ITuple tuple : tuples) {
				collector.write(new Text(tuple.getString("title")), new Text(tuple.getString("content")));
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
		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
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
