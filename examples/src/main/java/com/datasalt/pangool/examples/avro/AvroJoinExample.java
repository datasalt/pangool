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
package com.datasalt.pangool.examples.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.lib.input.AvroInputFormat;
import org.apache.avro.mapreduce.lib.output.AvroOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.tuplemr.serialization.AvroFieldSerialization;

/**
 * This example illustrates two things:
 * 
 * <ul>
 *  <li> How to use {@link AvroInputFormat} and {@link AvroOutputFormat}, that are 
 *  compliant with new Hadoop's API : mapreduce.lib.{input,output} 
 *  </li>
 *  <li> How to perform a reduce-join with Avro-data using custom serialization</li> 
 *
 */
public class AvroJoinExample extends BaseExampleJob {

	@SuppressWarnings("serial")
	private static class TweetsMapper extends TupleMapper<AvroWrapper<Record>,NullWritable> {

		private Tuple tuple;
		public void setup(TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("tweet"));
		};
		
		public void map(AvroWrapper<Record> key, NullWritable value, TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {
			Record tweet = key.datum();
			tuple.set("tweet_id",tweet.get("id"));
			tuple.set("tweet_hashtags",tweet.get("hashtags"));
			collector.write(tuple);
		}
	}
	
	@SuppressWarnings("serial")
	private static class RetweetsMapper extends TupleMapper<LongWritable,Text> {
		private Tuple tuple;
		public void setup(TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("retweet"));
		};
		
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			tuple.set("username",tokens[0]);
			tuple.set("tweet_id",Integer.parseInt(tokens[1]));
			collector.write(tuple);
		}
	}
	

	@SuppressWarnings("serial")
	public static class Red extends TupleReducer<AvroWrapper<Record>, NullWritable> {

		private Record outputRecord;
		private AvroWrapper<Record> wrapper;
		
		public void setup(TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {
			outputRecord= new Record(getAvroOutputSchema());
			wrapper = new AvroWrapper<Record>();
		};
		@SuppressWarnings("unchecked")
    @Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			
			for(ITuple tuple : tuples) {
				if ("tweet".equals(tuple.getSchema().getName())){
					Array<String> hashtags = (Array<String>)tuple.get("tweet_hashtags");
					outputRecord.put("hashtags",hashtags);
				} else {
					String user = tuple.get("username").toString();
					outputRecord.put("username",user);
					wrapper.datum(outputRecord);
					collector.write(wrapper,NullWritable.get());
				}
			}
		}
	}

	public AvroJoinExample() {
		super("Usage: AvroJoinExample [input_path] [output_path]");
	}

	private static Schema getPangoolTweetSchema() {
		Field tweetIdField = Field.create("tweet_id",Schema.Field.Type.INT);
		Field tweetHashTags = Field.createObject("tweet_hashtags",Array.class);
		tweetHashTags.setSerialization(AvroFieldSerialization.class);
		tweetHashTags.addProp("avro.schema",getAvroStringArraySchema().toString());
		return new Schema("tweet",Arrays.asList(tweetIdField,tweetHashTags));
	}
	
	private static Schema getPangoolRetweetSchema(){
		Field userId = Field.create("username",Schema.Field.Type.STRING);
		Field tweetId = Field.create("tweet_id",Schema.Field.Type.INT);
		return new Schema("retweet",Arrays.asList(userId,tweetId));
	}
	
	
	
	public static org.apache.avro.Schema getAvroStringArraySchema(){
		return org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.STRING));
	}
	
	public static org.apache.avro.Schema getAvroTweetSchema(){
		List<org.apache.avro.Schema.Field> avroFields = 
				new ArrayList<org.apache.avro.Schema.Field>();
		avroFields.add(
				new org.apache.avro.Schema.Field("id",org.apache.avro.Schema.create(Type.INT),null,null));
		avroFields.add(
				new org.apache.avro.Schema.Field("text",org.apache.avro.Schema.create(Type.STRING),null,null));
		avroFields.add(
				new org.apache.avro.Schema.Field("timestamp",org.apache.avro.Schema.create(Type.LONG),null,null));
		avroFields.add(new org.apache.avro.Schema.Field("hashtags",getAvroStringArraySchema(),null,null));
		org.apache.avro.Schema result= org.apache.avro.Schema.createRecord("tweet",null,null,false);
		result.setFields(avroFields);
		return result;
	}
	
	public static org.apache.avro.Schema getAvroOutputSchema(){
		org.apache.avro.Schema.Field retweeter = 
				new org.apache.avro.Schema.Field("username",
						org.apache.avro.Schema.create(Type.STRING),null,null);
		org.apache.avro.Schema.Field tweet = 
				new org.apache.avro.Schema.Field("hashtags",getAvroStringArraySchema(),null,null);
		
		org.apache.avro.Schema result= org.apache.avro.Schema.createRecord("output",null,null,false);
		result.setFields(Arrays.asList(retweeter,tweet));
		return result;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			failArguments("Wrong number of arguments");
			return -1;
		}
		Path tweetsPath = new Path(args[0]);
		Path retweetsPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		deleteOutput(outputPath.toString());
		
		

		TupleMRBuilder mr = new TupleMRBuilder(conf, "AvroJoinExample");
		mr.addIntermediateSchema(getPangoolTweetSchema());
		mr.addIntermediateSchema(getPangoolRetweetSchema());
		mr.setGroupByFields("tweet_id");
		//here the custom comparator that groups by "topic,word" is used. 
		//MyAvroComparator customComp = new MyAvroComparator(getTweetSchema(),"topic","word");
		
		mr.getConf().set(AvroJob.INPUT_SCHEMA,getAvroTweetSchema().toString());
		mr.getConf().set(AvroJob.OUTPUT_SCHEMA,getAvroOutputSchema().toString());
		mr.addInput(tweetsPath, new HadoopInputFormat(AvroInputFormat.class), new TweetsMapper());
		mr.addInput(retweetsPath, new HadoopInputFormat(TextInputFormat.class), new RetweetsMapper());
		mr.setOutput(outputPath,new HadoopOutputFormat(AvroOutputFormat.class),AvroWrapper.class,NullWritable.class);

		mr.setTupleReducer(new Red());

		mr.createJob().waitForCompletion(true);

		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AvroJoinExample(), args);
	}
}
