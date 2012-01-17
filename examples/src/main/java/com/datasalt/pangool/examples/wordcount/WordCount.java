package com.datasalt.pangool.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;

public class WordCount {

	private static final String WORD_FIELD = "word";
	
	public static class Split extends InputProcessor<LongWritable, Text>{
		Tuple tuple = new Tuple();
		
		@Override
    public void process(LongWritable key, Text value, Collector collector) throws IOException, InterruptedException {
			String words[] = value.toString().split("\\s+");
			for(String word : words) {
				tuple.setString(WORD_FIELD, word);
				collector.write(tuple);
			}
    }
	}
	
	public static class Count extends GroupHandler<Text, Text> {

		@Override
    public void onGroupElements(ITuple group, Iterable<ITuple> tuples, State state, Context context) throws IOException,
        InterruptedException, CoGrouperException {
			int count = 0;
			for (ITuple tuple : tuples) {
				count ++;
			}
			context.write(new Text(group.getString(WORD_FIELD)), new IntWritable(count));			
    }
	}

	private static final String HELP = "Usage: WordCount [input_path] [output_path]";
	
	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fs, new Path(args[1])); 
		
		PangoolConfig config = new PangoolConfigBuilder()
    .addSchema(0, Schema.parse(WORD_FIELD + ":string"))
    .setGroupByFields(WORD_FIELD)
    .setSorting(new SortingBuilder().add(WORD_FIELD).buildSorting())
    .build();
			
		CoGrouper cg = new CoGrouper(config, conf);
		cg.setJarByClass(WordCount.class);
		cg.addInput(new Path(args[0]), TextInputFormat.class, Split.class);
		cg.setOutput(new Path(args[1]), TextOutputFormat.class, Text.class, Text.class);
		cg.setGroupHandler(Count.class);
		
		cg.createJob().waitForCompletion(true);
	}
}
