package com.datasalt.pangool.examples.wordcount;

import java.io.IOException;
import java.nio.charset.Charset;
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
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.api.CombinerHandler;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.api.InputProcessor.CoGrouperContext;
import com.datasalt.pangool.api.InputProcessor.Collector;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;

public class WordCount {

	private static final int WORD_FIELD = 0;
	private static final int COUNT_FIELD = 1;
	public final static Charset UTF8 = Charset.forName("UTF-8");
	
	@SuppressWarnings("serial")
	public static class Split extends InputProcessor<LongWritable, Text> {

		private Tuple tuple;
		
		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			Schema schema = context.getCoGrouperConfig().getSource(0);
			this.tuple = new Tuple(schema);
		}
		
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

		private Tuple tuple;
		
		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			Schema schema = context.getCoGrouperConfig().getSource(0);
			this.tuple = new Tuple(schema);
		}

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			int count = 0;
			tuple.setString(WORD_FIELD, group.getString(WORD_FIELD));
			for(ITuple tuple : tuples) {
				count += (Integer) tuple.getInt(1);
			}
			tuple.setInt(COUNT_FIELD, count);
			collector.write(this.tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Count extends GroupHandler<Text, IntWritable> {

		IntWritable countToEmit;
		Text text;
		
		public void setup(CoGrouperContext coGrouperContext, Collector collector) throws IOException, InterruptedException,
		    CoGrouperException {
			countToEmit = new IntWritable();
			text = new Text();
		};

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			int count = 0;
			for(ITuple tuple : tuples) {
				count += (Integer) tuple.getInt(1);
			}
			countToEmit.set(count);
			text.set(group.getString(WORD_FIELD));
			collector.write(text, countToEmit);
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws InvalidFieldException, CoGrouperException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		CoGrouperConfigBuilder config = new CoGrouperConfigBuilder();
		config.addSchema(0, Schema.parse("word:string, count:int"));
		config.setGroupByFields("word");
		config.setSorting(new SortingBuilder().add("word").buildSorting()).build();

		CoGrouper cg = new CoGrouper(config.build(), conf);
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
