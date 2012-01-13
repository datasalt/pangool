package com.datasalt.pangool.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.io.tuple.BaseTuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.mapreduce.GroupHandler;
import com.datasalt.pangool.mapreduce.GroupHandler.State;
import com.datasalt.pangool.mapreduce.InputProcessor;
import com.datasalt.pangool.mapreduce.InputProcessor.Collector;

public class WordCount {

	private static final String WORD_FIELD = "word";
	
	public static class Split extends InputProcessor<LongWritable, Text>{
		BaseTuple tuple = new BaseTuple();
		
		@Override
    public void process(LongWritable key, Text value, Collector collector) throws IOException, InterruptedException,
        GrouperException {
			String words[] = value.toString().split("\\s+");
			for(String word : words) {
				tuple.setString(WORD_FIELD, word);
				collector.write(tuple);
			}
    }
	}
	
	public static class Count extends GroupHandler<Text, Text> {

		@Override
    public void onGroupElements(Iterable<ITuple> tuples, State state, Context context) throws IOException,
        InterruptedException, CoGrouperException {
			int count = 1;
			ITuple tuple = tuples.iterator().next();
			String word = tuple.getString(WORD_FIELD);
			for (ITuple tup2le : tuples) {
				count ++;
			}
			context.write(new Text(word), new IntWritable(count));			
    }
	}

	private static final String HELP = "Usage: WordCount [path]";
	
	public static void main(String args[]) {
		if (args.length != 1) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}
		
		
	}
}
