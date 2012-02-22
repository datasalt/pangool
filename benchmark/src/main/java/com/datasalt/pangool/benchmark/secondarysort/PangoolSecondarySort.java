package com.datasalt.pangool.benchmark.secondarysort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.cogroup.CoGrouper;
import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.cogroup.processors.GroupHandler;
import com.datasalt.pangool.cogroup.processors.InputProcessor;
import com.datasalt.pangool.cogroup.processors.GroupHandler.CoGrouperContext;
import com.datasalt.pangool.cogroup.processors.GroupHandler.Collector;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * Code for solving a secondary sort problem with Pangool.
 * <p>
 * The secondary sort problem is: We have a file with sales registers: {departmentId nameId timestamp saleValue}. We
 * want to obtain meaningful statistics grouping by all people who perform sales (departmentId+nameId). We want to
 * obtain total sales value for certain periods of time, therefore we need to registers in each group to come sorted by
 * "timestamp".
 */
public class PangoolSecondarySort {

	@SuppressWarnings("serial")
	public static class IProcessor extends InputProcessor<LongWritable, Text> {
		private Tuple tuple;

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {
			if(tuple == null) {
				tuple = new Tuple(context.getCoGrouperConfig().getSourceSchema(0));
			}

			String[] fields = value.toString().trim().split("\t");
			tuple.set("intField", Integer.parseInt(fields[0]));
			tuple.set("strField", fields[1]);
			tuple.set("longField", Long.parseLong(fields[2]));
			tuple.set("doubleField", Double.parseDouble(fields[3]));
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Handler extends GroupHandler<Text, DoubleWritable> {

		private Text outputKey;
		private DoubleWritable outputValue;

		public void setup(CoGrouperContext coGrouperContext, Collector collector)
    throws IOException, InterruptedException, CoGrouperException {
			outputKey  = new Text();
			outputValue = new DoubleWritable();
		}
		
		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			String groupStr = group.get("intField") + "\t" + group.get("strField");
			double accumPayments = 0;
			for(ITuple tuple : tuples) {
				accumPayments += (Double) tuple.get("doubleField");
			}
			outputValue.set(accumPayments);
			outputKey.set(groupStr);
			collector.write(outputKey, outputValue);
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException {
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("intField", Integer.class));
		fields.add(new Field("strField", String.class));
		fields.add(new Field("longField", Long.class));
		fields.add(new Field("doubleField", Double.class));
		Schema schema = new Schema("schema", fields);

		CoGrouper grouper = new CoGrouper(conf,"Pangool Secondary Sort");
		grouper.addSourceSchema(schema);
		grouper.setGroupByFields("intField", "strField");
		grouper.setOrderBy(new SortBy().add("intField", Order.ASC).add("strField", Order.ASC)
		    .add("longField", Order.ASC));
		grouper.setGroupHandler(new Handler());
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, DoubleWritable.class);
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
		new PangoolSecondarySort().getJob(conf, input, output).waitForCompletion(true);
	}
}
