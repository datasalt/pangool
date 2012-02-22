package com.datasalt.pangool.benchmark.largestword;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.datasalt.pangool.cogroup.CoGrouper;
import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.cogroup.processors.GroupHandler;
import com.datasalt.pangool.cogroup.processors.InputProcessor;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.io.tuple.Schema.Field;

/**
 * 
 */
public class LargestWordCustomComparator {

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
			
			while(itr.hasMoreTokens()) {
				String word = itr.nextToken();
				
				tuple.set("word", word);
				collector.write(tuple);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class Count extends GroupHandler<Text, NullWritable> {
		private NullWritable n;
		
		public void setup(CoGrouperContext coGrouperContext, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			n = NullWritable.get();
		}
		
		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			
			for(ITuple tuple : tuples) {
				Text t = (Text)tuple.get("word");
				collector.write(t,n);
			}
		}
	}
	
	private static class MyUtf8Comparator implements RawComparator<Text>,Serializable {
		@Override
    public int compare(Text arg0, Text arg1) {
	    throw new NotImplementedException();
    }

		@Override
    public int compare(byte[] buf1, int off1, int len1, byte[] buf2, int off2,int len2) {
	    if (len1 > len2){
	    	return 1;
	    } else if (len1 < len2){
	    	return -1;
	    } else {
	    	return 0;
	    }
    }
	}
	

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("word",String.class));
		Schema schema = new Schema("schema",fields);

		CoGrouper cg = new CoGrouper(conf,"Largest Word using custom comparator");
		cg.addSourceSchema(schema);
		cg.setGroupByFields("word");
		cg.setOrderBy(new SortBy().add("word",Order.DESC,new MyUtf8Comparator()));
		cg.setJarByClass(LargestWordCustomComparator.class);
		cg.addInput(new Path(input), TextInputFormat.class, new Split());
		cg.setOutput(new Path(output), TextOutputFormat.class, Text.class,NullWritable.class);
		cg.setGroupHandler(new Count());
		return cg.createJob();
	}

	private static final String HELP = "Usage: [input_path] [output_path]";

	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Logger root = Logger.getRootLogger();
		root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		Configuration conf = new Configuration();
		new LargestWordCustomComparator().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}
