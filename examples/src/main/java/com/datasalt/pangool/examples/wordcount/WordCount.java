package com.datasalt.pangool.examples.wordcount;

import java.io.IOException;
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
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.api.CombinerHandler;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;

public class WordCount {

	private static final String WORD_FIELD = "word";
	private static final String COUNT_FIELD = "count";

	@SuppressWarnings("serial")
	public static class Split extends InputProcessor<LongWritable, Text> {

		Tuple tuple = new Tuple();

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			tuple.setInt(COUNT_FIELD, 1);
			while(itr.hasMoreTokens()) {
				tuple.setString(WORD_FIELD, itr.nextToken());
				collector.write(tuple);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class CountCombiner extends CombinerHandler {

		Tuple tuple = new Tuple();

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			int count = 0;
			this.tuple.setString(WORD_FIELD, group.getString(WORD_FIELD));
			for(ITuple tuple : tuples) {
				count += tuple.getInt(COUNT_FIELD);
			}
			this.tuple.setInt(COUNT_FIELD, count);
			collector.write(this.tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Count extends GroupHandler<Text, IntWritable> {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			int count = 0;
			for(ITuple tuple : tuples) {
				count += tuple.getInt(COUNT_FIELD);
			}
			collector.write(new Text(group.getString(WORD_FIELD)), new IntWritable(count));
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws InvalidFieldException, CoGrouperException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		CoGrouperConfig config = new CoGrouperConfigBuilder()
		    .addSchema(0, Schema.parse(WORD_FIELD + ":string, " + COUNT_FIELD + ":int")).setGroupByFields(WORD_FIELD)
		    .setSorting(new SortingBuilder().add(WORD_FIELD).buildSorting()).build();

		CoGrouper cg = new CoGrouper(config, conf);
		cg.setJarByClass(WordCount.class);
		cg.addInput(new Path(input), TextInputFormat.class, new Split());
		cg.setOutput(new Path(output), TextOutputFormat.class, Text.class, Text.class);
		cg.setGroupHandler(new Count());
		cg.setCombinerHandler(new CountCombiner());

		return cg.createJob();
	}

	private static final String HELP = "Usage: WordCount [input_path] [output_path]";

	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new WordCount().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}
