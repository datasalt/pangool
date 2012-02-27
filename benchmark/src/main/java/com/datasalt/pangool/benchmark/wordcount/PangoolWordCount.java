/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import com.datasalt.pangool.cogroup.TupleMRBuilder;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.processors.TupleCombiner;
import com.datasalt.pangool.cogroup.processors.TupleMapper;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Schema.Field.Type;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * Code for solving the simple PangoolWordCount problem in Pangool.
 */
public class PangoolWordCount {

	public final static Charset UTF8 = Charset.forName("UTF-8");
	
	@SuppressWarnings("serial")
	public static class Split extends TupleMapper<LongWritable, Text> {

		private Tuple tuple;
		
		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			if (tuple == null){
				tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
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
	public static class CountCombiner extends TupleCombiner {

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
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
	public static class Count extends TupleReducer<Text, IntWritable> {
		private IntWritable outputCount;
		
		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			
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

	public Job getJob(Configuration conf, String input, String output) throws TupleMRException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("word",Type.STRING));
		fields.add(Field.create("count",Type.INT));
		Schema schema = new Schema("schema",fields);

		TupleMRBuilder cg = new TupleMRBuilder(conf,"Pangool WordCount");
		cg.addIntermediateSchema(schema);
		cg.setGroupByFields("word");
		cg.setJarByClass(PangoolWordCount.class);
		cg.addInput(new Path(input), TextInputFormat.class, new Split());
		cg.setOutput(new Path(output), TextOutputFormat.class, Text.class, Text.class);
		cg.setTupleReducer(new Count());
		cg.setTupleCombiner(new CountCombiner());

		return cg.createJob();
	}

	private static final String HELP = "Usage: PangoolWordCount [input_path] [output_path]";

	public static void main(String args[]) throws TupleMRException, IOException, InterruptedException,
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
