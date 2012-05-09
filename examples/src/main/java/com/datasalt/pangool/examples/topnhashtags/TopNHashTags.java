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
package com.datasalt.pangool.examples.topnhashtags;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.examples.topnhashtags.Beans.HashTag;
import com.datasalt.pangool.examples.topnhashtags.Beans.SimpleTweet;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleRollupReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

/**
 * This example shows an advanced use of the Rollup feature for calculating the top N hashtags from a set of tweets
 * per each (location, date) pair.
 */
public class TopNHashTags extends BaseExampleJob {

	@SuppressWarnings("serial")
	private static class TweetsProcessor extends TupleMapper<LongWritable, Text> {

		private ObjectMapper jsonMapper;
		private Tuple tuple;

		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			jsonMapper = new ObjectMapper();
			jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("my_schema"));
		}

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {

			SimpleTweet tweet = jsonMapper.readValue(value.toString(), SimpleTweet.class);
			DateTime dateTime = new DateTime(tweet.getCreated_at_date());
			tuple.set("date", dateTime.getYear() + "-" + dateTime.getMonthOfYear() + "-" + dateTime.getDayOfMonth());
			tuple.set("location", tweet.getUser().getLocation());
			for(HashTag hashTag : tweet.getEntities().getHashtags()) {
				tuple.set("hashtag", hashTag.getText());
				tuple.set("count", 1);
				collector.write(tuple);
			}
		}
	}
	
	@SuppressWarnings("serial")
	public static class TweetsHandler extends TupleRollupReducer<Text, NullWritable> {

		int totalCount = 0;
		int n;
		PriorityQueue<HashTagCount> topNHashtags;
		Text textToEmit;

		static class HashTagCount implements Comparable<HashTagCount> {
			String hashTag;
			Integer count;

			@Override
			public int compareTo(HashTagCount arg) {
				return count.compareTo(arg.count);
			}
		}

		public TweetsHandler(int n) {
			this.n = n;
			topNHashtags = new PriorityQueue<HashTagCount>(n);
		}

		public void onCloseGroup(int depth, String field, ITuple lastElement, TupleMRContext context, Collector collector) throws IOException ,InterruptedException ,TupleMRException {
			if(field.equals("hashtag")) {
				// Add the count for this hashtag to the top-n Heap
				HashTagCount hashTagCount = new HashTagCount();
				hashTagCount.hashTag = lastElement.get("hashtag").toString();
				hashTagCount.count = totalCount;
				topNHashtags.add(hashTagCount);
				if(topNHashtags.size() > n) { // remove one element from the Heap if there are too many
					topNHashtags.poll();
				}
				totalCount = 0;
			} else if(field.equals("date")) {
				// Flush the top N
				while(!topNHashtags.isEmpty()) {
					if(textToEmit == null) {
						textToEmit = new Text();
					}
					HashTagCount hashTagCount = topNHashtags.poll();
					textToEmit.set(lastElement.get("location") + "\t" + lastElement.get("date") + "\t" + hashTagCount.hashTag
					    + "\t" + hashTagCount.count);
					collector.write(textToEmit, NullWritable.get());
				}
			}
		};

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			for(ITuple tuple : tuples) {
				totalCount += (Integer) tuple.get("count");
			}
		}
	}

	public TopNHashTags() {
		super("Usage: [input_path] [output_path] [n] . The n parameter is the size of the top hashtags to be calculated.");
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			failArguments("Invalid number of arguments");
			return -1;
		}
		String input = args[0];
		String output = args[1];
		int n = Integer.parseInt(args[2]);
		
		delete(output);
		
		// Configure schema, sort and group by
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("location", Type.STRING));
		fields.add(Field.create("date", Type.STRING));
		fields.add(Field.create("hashtag", Type.STRING));
		fields.add(Field.create("count", Type.INT));
		Schema schema = new Schema("my_schema", fields);

		TupleMRBuilder mr = new TupleMRBuilder(conf);
		mr.addIntermediateSchema(schema);
		mr.setGroupByFields("location", "date", "hashtag");
		mr.setOrderBy(new OrderBy().add("location", Order.ASC).add("date", Order.ASC).add("hashtag", Order.ASC));
		mr.setRollupFrom("date");
		// Input / output and such
		mr.setTupleReducer(new TweetsHandler(n));
		mr.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		mr.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new TweetsProcessor());
		mr.createJob().waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TopNHashTags(), args);
	}
}
