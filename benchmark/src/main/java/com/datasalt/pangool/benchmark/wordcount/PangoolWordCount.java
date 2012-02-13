package com.datasalt.pangool.benchmark.wordcount;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.api.CombinerHandler;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * Code for solving the simple PangoolWordCount problem in Pangool.
 */
public class PangoolWordCount {

	public final static Charset UTF8 = Charset.forName("UTF-8");
	
	@SuppressWarnings("serial")
	public static class Split extends InputProcessor<LongWritable, Text> {

		private Tuple tuple;
		
		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {
			if (tuple == null){
				tuple = new Tuple(context.getCoGrouperConfig().getSourceSchema(0));
			}
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			tuple.set(1, 1);
			while(itr.hasMoreTokens()) {
				tuple.set(0, itr.nextToken());
				collector.write(tuple);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class CountCombiner extends CombinerHandler {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			int count = 0;
			ITuple outputTuple=null;
			for(ITuple tuple : tuples) {
				outputTuple = tuple;
				count += (Integer) tuple.get(1);
			}
			outputTuple.set(1, count);
			collector.write(outputTuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Count extends GroupHandler<Text, IntWritable> {
		private IntWritable outputCount;
		
		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			
			if(outputCount == null) {
				outputCount = new IntWritable();
			}
			int count = 0;
			for(ITuple tuple : tuples) {
				count += (Integer) tuple.get(1);
			}
			outputCount.set(count);
			collector.write((Text)group.get(0), outputCount);
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws InvalidFieldException, CoGrouperException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("word",String.class));
		fields.add(new Field("count",Integer.class));
		Schema schema = new Schema("schema",fields);

		CoGrouper cg = new CoGrouper(conf);
		cg.addSourceSchema(schema);
		cg.setGroupByFields("word");
		cg.setJarByClass(PangoolWordCount.class);
		cg.addInput(new Path(input), TextInputFormat.class, new Split());
		cg.setOutput(new Path(output), TextOutputFormat.class, Text.class, Text.class);
		cg.setGroupHandler(new Count());
		cg.setCombinerHandler(new CountCombiner());

		return cg.createJob();
	}

	private static final String HELP = "Usage: PangoolWordCount [input_path] [output_path]";

	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new PangoolWordCount().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}
