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
package com.datasalt.pangool.examples.topicalwordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

/**
 * This example is an extended wordcount. It counts the total appearances for each word within a topic.
 * Input data is JSON registers with "text" and "topicId". This example shows how easy it is to work with
 * compound registers in Pangool, specifically how easy it is to "group by" more than one field.
 */
public class TopicalWordCount extends BaseExampleJob {

	@SuppressWarnings("serial")
	public static class TokenizeMapper extends TupleMapper<LongWritable, Text> {

		protected Tuple tuple;
		protected ObjectMapper mapper;

		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			this.mapper = new ObjectMapper();
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
		};

		@SuppressWarnings("rawtypes")
		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {

			// Parse the JSON
			Map document = mapper.readValue(value.toString(), Map.class);
			// Set topic Id
			tuple.set("topic", (Integer) document.get("topicId"));
			// Tokenize the text
			StringTokenizer itr = new StringTokenizer((String) document.get("text"));
			tuple.set("count", 1);
			while(itr.hasMoreTokens()) {
				tuple.set("word", itr.nextToken());
				emitTuple(collector); // We are creating a method for this to allow overriding it from subclasses
			}
		}

		protected void emitTuple(Collector collector) throws IOException, InterruptedException {
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class CountReducer extends TupleReducer<ITuple, NullWritable> {

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			int count = 0;
			ITuple outputTuple = null;
			for(ITuple tuple : tuples) {
				count += (Integer) tuple.get("count");
				outputTuple = tuple;
			}
			outputTuple.set("count", count);
			collector.write(outputTuple, NullWritable.get());
		}
	}

	public TopicalWordCount() {
		super("Usage: TopicalWordCount [input_path] [output_path]");
	}

	static Schema getSchema() {
		List<Field> fields = new ArrayList<Field>();
		// The schema has 3 fields: word, topicId and count
		fields.add(Field.create("word", Type.STRING));
		fields.add(Field.create("topic", Type.INT));
		fields.add(Field.create("count", Type.INT));
		return new Schema("schema", fields);
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Wrong number of arguments");
			return -1;
		}

		delete(args[1]);

		TupleMRBuilder mr = new TupleMRBuilder(conf, "Pangool Topical Word Count");
		mr.addIntermediateSchema(getSchema());
		// We will count each (topicId, word) pair
		// Note that the order in which we defined the fields of the Schema is not relevant here
		mr.setGroupByFields("topic", "word");
		mr.addInput(new Path(args[0]), new HadoopInputFormat(TextInputFormat.class), new TokenizeMapper());
		// We'll use a TupleOutputFormat with the same schema than the intermediate schema
		mr.setTupleOutput(new Path(args[1]), getSchema());
		mr.setTupleReducer(new CountReducer());
		mr.setTupleCombiner(new CountReducer());

		mr.createJob().waitForCompletion(true);

		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TopicalWordCount(), args);
	}
}
