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
package com.datasalt.pangool.benchmark.sansaccentsorting;

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

import com.datasalt.pangool.cogroup.TupleMRBuilder;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.cogroup.processors.TupleMapper;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.io.tuple.Schema.Field;

/**
 * 
 */
public class SansAccentRepeatedField {

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
	public static class Count extends TupleReducer<Text, NullWritable> {
		private NullWritable n;
		
		public void setup(TupleMRContext coGrouperContext, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			n = NullWritable.get();			
		}
		
		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			
			for(ITuple tuple : tuples) {
				Text t = (Text)tuple.get("word");
				collector.write(t,n);
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws TupleMRException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("word",Utf8.class));
		fields.add(new Field("encoded_word",Utf8.class));
		Schema schema = new Schema("schema",fields);

		TupleMRBuilder cg = new TupleMRBuilder(conf,"Utf8 Alternate order repeating fields");
		cg.addIntermediateSchema(schema);
		cg.setGroupByFields("encoded_word");
		//cg.setOrderBy(new SortBy().add("encoded_word",Order.ASC).add("word",Order.DESC));
		cg.setJarByClass(SansAccentRepeatedField.class);
		cg.addInput(new Path(input), TextInputFormat.class, new Split());
		cg.setOutput(new Path(output), TextOutputFormat.class, Text.class,NullWritable.class);
		cg.setTupleReducer(new Count());
		return cg.createJob();
	}

	private static final String HELP = "Usage: [input_path] [output_path]";

	public static void main(String args[]) throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new SansAccentRepeatedField().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}
