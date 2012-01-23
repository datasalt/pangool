package com.datasalt.pangool.examples.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.Schema;
import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Sorting;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * Like original Hadoop's SecondarySort example. Reads a tabulated text file with two numbers, groups by the first and
 * sorts by both.
 * 
 * @author pere
 * 
 */
public class SecondarySort {

	public final static String FIRST = "first";
	public final static String SECOND = "second";

	public static class IProcessor extends InputProcessor<LongWritable, Text> {

		Tuple tuple = new Tuple();

		@Override
		public void process(LongWritable key, Text value, Collector collector) throws IOException, InterruptedException {

			String[] fields = value.toString().trim().split(" ");
			tuple.setInt(FIRST, Integer.parseInt(fields[0]));
			tuple.setInt(SECOND, Integer.parseInt(fields[1]));
			collector.write(tuple);
		}
	}

	public static class Handler extends GroupHandler<Text, NullWritable> {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, State state,
		    ReduceContext<ITuple, NullWritable, Text, NullWritable> context) throws IOException, InterruptedException,
		    CoGrouperException {

			for(ITuple tuple : tuples) {
				context.write(new Text(tuple.getInt(FIRST) + "\t" + tuple.getInt(SECOND)), NullWritable.get());
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException {
		// Configure schema, sort and group by
		Schema schema = Schema.parse(FIRST + ":int, " + SECOND + ":int");
		Sorting sort = Sorting.parse(FIRST + " asc, " + SECOND + " asc");
		PangoolConfig config = new PangoolConfigBuilder().addSchema(0, schema).setGroupByFields(FIRST).setSorting(sort)
		    .build();

		CoGrouper grouper = new CoGrouper(config, conf);
		// Input / output and such
		grouper.setGroupHandler(Handler.class);
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		grouper.addInput(new Path(input), TextInputFormat.class, IProcessor.class);
		return grouper.createJob();
	}
}
