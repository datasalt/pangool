package com.datasalt.pangool.benchmark.utf8sorting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.api.GroupHandler.CoGrouperContext;
import com.datasalt.pangool.api.GroupHandler.Collector;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * 
 */
public class Utf8EncodedRepeatedField {

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
				String encodedWord = AsciiUtils.convertNonAscii(word);
				tuple.set("word", word);
				tuple.set("encoded_word",encodedWord);
				collector.write(tuple);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class Count extends GroupHandler<Text, NullWritable> {
		private NullWritable n;
		//private Text separator;
		
		public void setup(CoGrouperContext coGrouperContext, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			n = NullWritable.get();
			//separator = new Text("\t");
			
		}
		
		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			
			for(ITuple tuple : tuples) {
				//text.set((Text)tuple.get("word"))
				Text t = (Text)tuple.get("word");
//				Text encoded = (Text)tuple.get("encoded_word");
//				t.append(separator.getBytes(),0,separator.getLength());
//				t.append(encoded.getBytes(),0,encoded.getLength());
				collector.write(t,n);
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws InvalidFieldException, CoGrouperException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("word",String.class));
		fields.add(new Field("encoded_word",String.class));
		Schema schema = new Schema("schema",fields);

		CoGrouper cg = new CoGrouper(conf,"Utf8 Alternate order repeating fields");
		cg.addSourceSchema(schema);
		cg.setGroupByFields("encoded_word");
		//cg.setOrderBy(new SortBy().add("encoded_word",Order.ASC).add("word",Order.DESC));
		cg.setJarByClass(Utf8EncodedRepeatedField.class);
		cg.addInput(new Path(input), TextInputFormat.class, new Split());
		cg.setOutput(new Path(output), TextOutputFormat.class, Text.class,NullWritable.class);
		cg.setGroupHandler(new Count());
		return cg.createJob();
	}

	private static final String HELP = "Usage: Utf8EncodedRepeated [input_path] [output_path]";

	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new Utf8EncodedRepeatedField().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}
