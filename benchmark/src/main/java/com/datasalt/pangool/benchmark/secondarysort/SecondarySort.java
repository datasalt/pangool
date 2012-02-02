package com.datasalt.pangool.benchmark.secondarysort;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Sorting;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * Code for solving a secondary sort problem with Pangool.
 * <p>
 * The secondary sort problem is: We have a file with sales registers: {departmentId nameId timestamp saleValue}. We
 * want to obtain meaningful statistics grouping by all people who perform sales (departmentId+nameId). We want to
 * obtain total sales value for certain periods of time, therefore we need to registers in each group to come sorted by
 * "timestamp".
 */
public class SecondarySort {

	public final static int INTFIELD = 0, STRINGFIELD = 1, LONGFIELD = 2, DOUBLEFIELD = 3;

	@SuppressWarnings("serial")
	public static class IProcessor extends InputProcessor<LongWritable, Text> {

		Tuple tuple = new Tuple(4);

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {

			String[] fields = value.toString().trim().split("\t");
			tuple.setInt(INTFIELD, Integer.parseInt(fields[0]));
			tuple.setString(STRINGFIELD, Utf8.getBytesFor(fields[1]));
			tuple.setLong(LONGFIELD, Long.parseLong(fields[2]));
			tuple.setDouble(DOUBLEFIELD, Double.parseDouble(fields[3]));
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Handler extends GroupHandler<Text, NullWritable> {

		Text result;

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			if(result == null) {
				result = new Text();
			}
			String groupStr = group.getInt(INTFIELD) + "\t" + new Utf8(group.getString(STRINGFIELD)).toString();
			double accumPayments = 0;
			for(ITuple tuple : tuples) {
				accumPayments += tuple.getDouble(DOUBLEFIELD);
			}
			result.set(groupStr + "\t" + accumPayments);
			collector.write(result, NullWritable.get());
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException {
		// Configure schema, sort and group by
		Schema schema = Schema.parse("intField:int, strField:string, longField:long, doubleField:double");
		Sorting sort = Sorting.parse("intField asc, strField asc, longField asc");
		CoGrouperConfigBuilder config = new CoGrouperConfigBuilder();
		config.addSchema(0, schema);
		config.setGroupByFields("intField", "strField");
		config.setSorting(sort).build();

		CoGrouper grouper = new CoGrouper(config.build(), conf);
		// Input / output and such
		grouper.setGroupHandler(new Handler());
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		grouper.addInput(new Path(input), TextInputFormat.class, new IProcessor());
		return grouper.createJob();
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException,
	    CoGrouperException {

		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		String input = args[0];
		String output = args[1];
		HadoopUtils.deleteIfExists(fS, new Path(output));
		new SecondarySort().getJob(conf, input, output).waitForCompletion(true);
	}
}
