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
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapreduce.lib.input.AvroInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

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
import com.datasalt.pangool.tuplemr.serialization.FieldAvroSerialization.AvroFieldDeserializer;
import com.datasalt.pangool.tuplemr.serialization.FieldAvroSerialization.AvroFieldSerializer;

/**
 * TODO STILL WIP!!
 * 
 *
 */
public class Avro2Job extends BaseExampleJob {

	@SuppressWarnings("serial")
	public static class TokenizeMapper extends TupleMapper<LongWritable, Text> {

		protected Tuple tuple;
		protected Record record;
		protected ObjectMapper mapper;

		public void setup(TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {
			this.mapper = new ObjectMapper();
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
			record = new Record(getAvroTweetSchema());
			tuple.set("my_avro",record);
		};

		@SuppressWarnings("rawtypes")
		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {

			Map document = mapper.readValue(value.toString(), Map.class);
			record.put("topic", (Integer) document.get("topicId"));
			StringTokenizer itr = new StringTokenizer((String) document.get("text"));
			record.put("count", 1);
			while(itr.hasMoreTokens()) {
				record.put("word", itr.nextToken());
				tuple.set("my_avro",record);
				collector.write(tuple);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class Red extends TupleReducer<ITuple, NullWritable> {

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			int count = 0;
			ITuple outputTuple = null;
			Record outputRecord=null;
			for(ITuple tuple : tuples) {
				Record record = (Record)tuple.get("tweet");
				count += (Integer) record.get("count");
				outputTuple = tuple;
				outputRecord = record;
			}
			outputRecord.put("count",count);
			outputTuple.set("my_avro",outputRecord);
			collector.write(outputTuple, NullWritable.get());
		}
	}

	public Avro2Job() {
		super("Usage: Avro2Job [input_path] [output_path]");
	}

	private static Schema getPangoolTweetsSchema() {
		org.apache.avro.Schema avroSchema = getAvroTweetSchema();
		Field tweetIdField = Field.create("tweet_id",Schema.Field.Type.INT);
		Field avroField = Field.createObject("tweet",AvroFieldSerializer.class,AvroFieldDeserializer.class);
		avroField.addProp("avro.schema",avroSchema.toString());
		return new Schema("retweeters",Arrays.asList(tweetIdField,avroField));
	}
	
	private static Schema getRetweetsSchema(){
		Field userId = Field.create("user_id",Schema.Field.Type.STRING);
		Field tweetId = Field.create("tweet_id",Schema.Field.Type.INT);
		return new Schema("tweets",Arrays.asList(userId,tweetId));
	}
	
	public static org.apache.avro.Schema getAvroTweetSchema(){
		List<org.apache.avro.Schema.Field> avroFields = 
				new ArrayList<org.apache.avro.Schema.Field>();
		avroFields.add(
				new org.apache.avro.Schema.Field("tweet_id",org.apache.avro.Schema.create(Type.STRING),null,null));
		avroFields.add(
				new org.apache.avro.Schema.Field("text",org.apache.avro.Schema.create(Type.INT),null,null));
		avroFields.add(
				new org.apache.avro.Schema.Field("timestamp",org.apache.avro.Schema.create(Type.INT),null,null));
		avroFields.add(new org.apache.avro.Schema.Field("hashTags",org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.STRING)),null,null));
		org.apache.avro.Schema result= org.apache.avro.Schema.createRecord("tweet",null,null,false);
		result.setFields(avroFields);
		return result;
	}
	
	public static org.apache.avro.Schema getAvroOutputSchema(){
		org.apache.avro.Schema.Field retweeter = 
				new org.apache.avro.Schema.Field("retweeter_id",
						org.apache.avro.Schema.create(Type.STRING),null,null);
		org.apache.avro.Schema.Field tweet = 
				new org.apache.avro.Schema.Field("tweet",getAvroTweetSchema(),null,null);
		
		org.apache.avro.Schema result= org.apache.avro.Schema.createRecord("tweet",null,null,false);
		result.setFields(Arrays.asList(retweeter,tweet));
		return result;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Wrong number of arguments");
			return -1;
		}
		Path tweetsPath = new Path(args[0]);
		Path retweetersPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		deleteOutput(outputPath.toString());
		
		

		TupleMRBuilder mr = new TupleMRBuilder(conf, "Pangool Avro Join");
		mr.addIntermediateSchema(getPangoolTweetsSchema());
		mr.addIntermediateSchema(getRetweetsSchema());
		mr.setGroupByFields("tweet_id");
		//here the custom comparator that groups by "topic,word" is used. 
		//MyAvroComparator customComp = new MyAvroComparator(getTweetSchema(),"topic","word");
		
		mr.addInput(tweetsPath, new HadoopInputFormat(AvroInputFormat.class), new TokenizeMapper());
		// We'll use a TupleOutputFormat with the same schema than the intermediate schema
		mr.setTupleOutput(new Path(args[1]), getPangoolTweetsSchema());
		mr.setTupleReducer(new Red());
		mr.setTupleCombiner(new Red());

		mr.createJob().waitForCompletion(true);

		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Avro2Job(), args);
	}
}
