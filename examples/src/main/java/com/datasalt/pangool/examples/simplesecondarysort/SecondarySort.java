package com.datasalt.pangool.examples.simplesecondarysort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.Criteria.Order;
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

	public final static int FIRST = 0;
	public final static int SECOND = 1;

	private static class IProcessor extends InputProcessor<LongWritable, Text> {

		private Schema schema;
		
		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			this.schema = context.getCoGrouperConfig().getSourceSchema("my_schema");
		}
		
		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {
			Tuple tuple = new Tuple(schema);
			String[] fields = value.toString().trim().split(" ");
			tuple.set(FIRST, Integer.parseInt(fields[0]));
			tuple.set(SECOND, Integer.parseInt(fields[1]));
			collector.write(tuple);
		}
	}

	public static class Handler extends GroupHandler<Text, NullWritable> {

		/**
     * 
     */
		private static final long serialVersionUID = 1L;

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			for(ITuple tuple : tuples) {
				collector.write(new Text(tuple.get(FIRST) + "\t" + tuple.get(SECOND)), NullWritable.get());
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException {
		// Configure schema, sort and group by
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("first",Integer.class));
		fields.add(new Field("second",Integer.class));
		
		Schema schema = new Schema("my_schema",fields);
		CoGrouper grouper = new CoGrouper(conf);
		grouper.addSourceSchema(schema);
		grouper.setGroupByFields("first");
		grouper.setOrderBy(new SortBy().add("first",Order.ASC).add("second",Order.ASC));
		// Input / output and such
		grouper.setGroupHandler(new Handler());
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		grouper.addInput(new Path(input), TextInputFormat.class, new IProcessor());
		return grouper.createJob();
	}
}
