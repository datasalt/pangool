package com.datasalt.pangool.examples.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
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
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector) throws IOException, InterruptedException {

			String[] fields = value.toString().trim().split(" ");
			tuple.setInt(FIRST, Integer.parseInt(fields[0]));
			tuple.setInt(SECOND, Integer.parseInt(fields[1]));
			collector.write(tuple);
		}
	}

	public static class Handler extends GroupHandler<Text, NullWritable> {

		/**
     * 
     */
    private static final long serialVersionUID = 1L;

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext<Text, NullWritable> context,
		    Collector<Text, NullWritable> collector) throws IOException, InterruptedException, CoGrouperException {

			for(ITuple tuple : tuples) {
				collector.write(new Text(tuple.getInt(FIRST) + "\t" + tuple.getInt(SECOND)), NullWritable.get());
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException {
		// Configure schema, sort and group by
		Schema schema = Schema.parse(FIRST + ":int, " + SECOND + ":int");
		Sorting sort = Sorting.parse(FIRST + " asc, " + SECOND + " asc");
		CoGrouperConfig config = new CoGrouperConfigBuilder().addSchema(0, schema).setGroupByFields(FIRST).setSorting(sort)
		    .build();

		CoGrouper grouper = new CoGrouper(config, conf);
		// Input / output and such
		grouper.setGroupHandler(new Handler());
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		grouper.addInput(new Path(input), TextInputFormat.class, IProcessor.class);
		return grouper.createJob();
	}
}
