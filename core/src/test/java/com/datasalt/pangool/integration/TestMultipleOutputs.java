package com.datasalt.pangool.integration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Sorting;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestMultipleOutputs extends AbstractHadoopTestLibrary {

	public final static String INPUT = TestMultipleOutputs.class.getName() + "-input";
	public final static String OUTPUT = TestMultipleOutputs.class.getName() + "-output";

	public final static String OUTPUT_1 = "out1";
	public final static String OUTPUT_2 = "out2";
	public final static String TUPLEOUTPUT_1 = "tuple1";

	public static class MyInputProcessor extends InputProcessor<LongWritable, Text> {

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {

			Tuple tuple = new Tuple();
			tuple.setString("name", "Pere");
			tuple.setInt("money", 100);
			tuple.setString("country", "ES");

			// We use the multiple outputs here -
			RecordWriter writer = collector.getNamedOutput(OUTPUT_1);
			System.out.println(writer);
			writer.write(new Text(tuple.getString("name")), new Text(tuple.getString("country")));
			writer = collector.getNamedOutput(OUTPUT_2);
			System.out.println(writer);
			writer.write(new IntWritable(tuple.getInt("money")), NullWritable.get());
			writer = collector.getNamedOutput(TUPLEOUTPUT_1);
			System.out.println(writer);
			writer.write(tuple, NullWritable.get());

			collector.write(tuple);
		}
	}

	public static class MyGroupHandler extends GroupHandler<DoubleWritable, NullWritable> {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples,
		    CoGrouperContext<DoubleWritable, NullWritable> pangoolContext, Collector<DoubleWritable, NullWritable> collector)
		    throws IOException, InterruptedException, CoGrouperException {

			for(ITuple tuple : tuples) {
				// We also use the multiple outputs here -
				collector.getNamedOutput(OUTPUT_1).write(new Text(tuple.getString("name")),
				    new Text(tuple.getString("country")));
				collector.getNamedOutput(OUTPUT_2).write(new IntWritable(tuple.getInt("money")), NullWritable.get());
				collector.getNamedOutput(TUPLEOUTPUT_1).write(tuple, NullWritable.get());
			}

			collector.write(new DoubleWritable(1.0), NullWritable.get());
		}
	}

	@Test
	public void test() throws InvalidFieldException, CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {

		initHadoop();
		trash(INPUT, OUTPUT);

		// Define schema and sorting
		Schema baseSchema = Schema.parse("name:string, money:int, country:string");
		Sorting sorting = Sorting.parse("country asc, name asc, money desc");

		CoGrouperConfigBuilder config = new CoGrouperConfigBuilder();
		config.setSchema(baseSchema);
		config.setGroupByFields("country");
		config.setSorting(sorting);

		CoGrouper coGrouper = new CoGrouper(config.build(), getConf());
		coGrouper.addInput(new Path(INPUT), TextInputFormat.class, MyInputProcessor.class);
		coGrouper.setGroupHandler(MyGroupHandler.class);
		coGrouper.setOutput(new Path(OUTPUT), SequenceFileOutputFormat.class, DoubleWritable.class, NullWritable.class);
		// Configure extra outputs
		coGrouper.addNamedOutput(OUTPUT_1, TextOutputFormat.class, Text.class, Text.class);
		coGrouper.addNamedOutput(OUTPUT_2, SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
		coGrouper.addNamedTupleOutput(TUPLEOUTPUT_1, baseSchema);

		Job job = coGrouper.createJob();
		// One file with one line - context will be ignored
		// Business logic in {@link MyInputProcessor}
		Files.write("ignore-me", new File(INPUT), Charset.forName("UTF-8"));
		job.waitForCompletion(true);

//		trash(INPUT, OUTPUT);
//		cleanUp();
	}
}
