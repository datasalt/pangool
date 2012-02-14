package com.datasalt.pangool.integration;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Fields;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.api.ProxyOutputFormat;
import com.datasalt.pangool.commons.CommonUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.test.AbstractHadoopTestLibrary;

public class TestMultipleOutputs extends AbstractHadoopTestLibrary {

	public final static String INPUT = TestMultipleOutputs.class.getName() + "-input";
	public final static String OUTPUT = TestMultipleOutputs.class.getName() + "-output";

	public final static String OUTPUT_1 = "out1";
	public final static String OUTPUT_2 = "out2";
	public final static String TUPLEOUTPUT_1 = "tuple1";

	@SuppressWarnings("serial")
  public static class MyInputProcessor extends InputProcessor<LongWritable, Text> {

		private Tuple tuple;
		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			tuple = new Tuple(context.getCoGrouperConfig().getSourceSchema(0));
		}
		
		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {
			tuple.set(0, "Pere");
			tuple.set(1, 100);
			tuple.set(2, "ES");

			// We use the multiple outputs here -
			collector.write(OUTPUT_1, new Text((String)tuple.get(0)), new Text((String)tuple.get(2)));
			collector.write(OUTPUT_2, new IntWritable((Integer)tuple.get(1)), NullWritable.get());
			collector.write(TUPLEOUTPUT_1, tuple, NullWritable.get());

			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
  public static class MyGroupHandler extends GroupHandler<DoubleWritable, NullWritable> {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples,
		    CoGrouperContext pangoolContext, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			for(ITuple tuple : tuples) {
				// We also use the multiple outputs here -
				collector.write(OUTPUT_1, tuple.get(0), tuple.get(2));
				collector.write(OUTPUT_2, new IntWritable((Integer)tuple.get(1)), NullWritable.get());
				collector.write(TUPLEOUTPUT_1, tuple, NullWritable.get());
			}

			collector.write(new DoubleWritable(1.0), NullWritable.get());
		}
	}

	@Test
	public void test() throws InvalidFieldException, CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException, InstantiationException, IllegalAccessException {

		initHadoop();
		trash(INPUT, OUTPUT);

		// One file with one line - context will be ignored
		// Business logic in {@link MyInputProcessor}
		CommonUtils.writeTXT("ignore-me", new File(INPUT));

		CoGrouper coGrouper = new CoGrouper(getConf());
		Schema baseSchema = new Schema("schema",Fields.parse("name:string, money:int, country:string"));
		coGrouper.addSourceSchema(baseSchema);
		coGrouper.setGroupByFields("country");
		coGrouper.setOrderBy(new SortBy().add("country",Order.ASC).add("money",Order.DESC).add("name",Order.ASC));
		coGrouper.addInput(new Path(INPUT), TextInputFormat.class, new MyInputProcessor());
		coGrouper.setGroupHandler(new MyGroupHandler());
		coGrouper.setOutput(new Path(OUTPUT), SequenceFileOutputFormat.class, DoubleWritable.class, NullWritable.class);
		// Configure extra outputs
		coGrouper.addNamedOutput(OUTPUT_1, SequenceFileOutputFormat.class, Text.class, Text.class);
		coGrouper.addNamedOutput(OUTPUT_2, SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
		coGrouper.addNamedTupleOutput(TUPLEOUTPUT_1, baseSchema);

		getConf()
		    .setClass(ProxyOutputFormat.PROXIED_OUTPUT_FORMAT_CONF, SequenceFileOutputFormat.class, OutputFormat.class);
		Job job = coGrouper.createJob();
		job.waitForCompletion(true);

		// Check outputs

		withOutput(firstReducerOutput(OUTPUT), new DoubleWritable(1.0), NullWritable.get());
		withOutput(firstReducerOutput(OUTPUT + "/" + OUTPUT_1), new Text("Pere"), new Text("ES"));
		withOutput(firstMapOutput(OUTPUT + "/" + OUTPUT_1), new Text("Pere"), new Text("ES"));
		withOutput(firstReducerOutput(OUTPUT + "/" + OUTPUT_2), new IntWritable(100), NullWritable.get());
		withOutput(firstMapOutput(OUTPUT + "/" + OUTPUT_2), new IntWritable(100), NullWritable.get());

		Tuple tuple = new Tuple(baseSchema);
		tuple.set(0, "Pere");
		tuple.set(1, 100);
		tuple.set(2, "ES");
		
		withTupleOutput(firstMapOutput(OUTPUT + "/" + TUPLEOUTPUT_1), tuple);
		withTupleOutput(firstReducerOutput(OUTPUT + "/" + TUPLEOUTPUT_1), tuple);
		
		trash(INPUT, OUTPUT);
		cleanUp();
	}
}
