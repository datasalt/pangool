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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.serialization.FieldAvroSerialization.AvroFieldDeserializer;
import com.datasalt.pangool.tuplemr.serialization.FieldAvroSerialization.AvroFieldSerializer;

/**
 * This is an advanced example to illustrate the usage of custom serializers and 
 * custom comparators.<br>
 * 
 * In this example the intermediate schema contains just a single Avro Record, whose avro schema is
 * "topic:int, word:string, count:int".<br> 
 * The custom serialization used is defined in {@link AvroFieldSerialization}. In addition to this,
 * a custom comparator {@link MyAvroComparator} is used to just compare and group by "topic , word".
 * <br>
 * The behaviour is identical to the example {@link TopicalWordCount} , but using the external
 * serialization provided by Avro. 
 */
public class AvroCustomSerializationJob extends BaseExampleJob {

	@SuppressWarnings("serial")
	public static class TokenizeMapper extends TupleMapper<LongWritable, Text> {

		protected Tuple tuple;
		protected Record record;
		protected ObjectMapper mapper;

		public void setup(TupleMRContext context, Collector collector) 
				throws IOException, InterruptedException {
			this.mapper = new ObjectMapper();
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
			record = new Record(getAvroSchema());
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
	public static class CountReducer extends TupleReducer<ITuple, NullWritable> {

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			int count = 0;
			ITuple outputTuple = null;
			Record outputRecord=null;
			for(ITuple tuple : tuples) {
				Record record = (Record)tuple.get("my_avro");
				count += (Integer) record.get("count");
				outputTuple = tuple;
				outputRecord = record;
			}
			outputRecord.put("count",count);
			outputTuple.set("my_avro",outputRecord);
			collector.write(outputTuple, NullWritable.get());
		}
	}

	public AvroCustomSerializationJob() {
		super("Usage: AvroCustomSerializationJob [input_path] [output_path]");
	}

	static Schema getSchema() {
		org.apache.avro.Schema avroSchema = getAvroSchema();
		Field avroField = Field.createObject("my_avro",AvroFieldSerializer.class,AvroFieldDeserializer.class);
		avroField.addProp("avro.schema",avroSchema.toString());
		return new Schema("schema",Arrays.asList(avroField));
	}
	
	static org.apache.avro.Schema getAvroSchema(){
		List<org.apache.avro.Schema.Field> avroFields = 
				new ArrayList<org.apache.avro.Schema.Field>();
		avroFields.add(
				new org.apache.avro.Schema.Field("word",org.apache.avro.Schema.create(Type.STRING),null,null));
		avroFields.add(
				new org.apache.avro.Schema.Field("topic",org.apache.avro.Schema.create(Type.INT),null,null));
		avroFields.add(
				new org.apache.avro.Schema.Field("count",org.apache.avro.Schema.create(Type.INT),null,null));
		org.apache.avro.Schema result= org.apache.avro.Schema.createRecord("avro_schema",null,null,false);
		result.setFields(avroFields);
		return result;
	}

	/**
	 * A custom comparator that deserializes bytes to Avro {@link Record} instances, and then
	 * compares by "topic" and "word" fields.
	 *
	 */
	@SuppressWarnings("serial")
  public static class MyAvroComparator implements RawComparator<Record>,Serializable {

		//MyAvroComparator must be serializable so 
		private transient AvroFieldDeserializer<Record> deser;
		private transient Record record1,record2;
		private transient DataInputBuffer inputBuffer;
		private String avroSchema;
		private String[] fields;
		public MyAvroComparator(org.apache.avro.Schema avroSchema,String ... fields){
			this.avroSchema = avroSchema.toString();
			this.fields = fields;
		}
		
		//lazy loading of deserializer and buffers
		private void init(){
			if (deser == null){
				deser = new AvroFieldDeserializer<Record>();
				Map<String,String> props = new HashMap<String,String>();
				props.put("avro.schema",avroSchema.toString());
				deser.setProps(props);
			}
			if (inputBuffer == null){
				inputBuffer = new DataInputBuffer();
			}
		}
		
		@Override
		@SuppressWarnings({"unchecked","rawtypes"})
    public int compare(Record record1, Record record2) {
	    for (String field : fields){
      int comparison =  ((Comparable)record1.get(field)).compareTo(record2.get(field));
	    	if (comparison != 0){
	    		return comparison;
	    	} 
	    }
	    return 0;    
	  }

		@Override
    public int compare(byte[] b1, int o1, int l1, byte[] b2, int o2, int l2) {
			init();
			try{
				inputBuffer.reset(b1,o1,l1);
				deser.open(inputBuffer);
				record1 = deser.deserialize(record1);
				deser.close();
				inputBuffer.reset(b2,o2,l2);
				deser.open(inputBuffer);
				record2 = deser.deserialize(record2);
				deser.close();
				return compare(record1, record2);
			} catch(IOException e){
				throw new PangoolRuntimeException(e);
			}
    }
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Wrong number of arguments");
			return -1;
		}

		deleteOutput(args[1]);

		TupleMRBuilder mr = new TupleMRBuilder(conf, "Pangool Topical Word Count");
		mr.addIntermediateSchema(getSchema());
		mr.setGroupByFields("my_avro");
		//here the custom comparator that groups by "topic,word" is used. 
		MyAvroComparator customComp = new MyAvroComparator(getAvroSchema(),"topic","word");
		mr.setOrderBy(new OrderBy().add("my_avro",Order.ASC,customComp));
		mr.addInput(new Path(args[0]), new HadoopInputFormat(TextInputFormat.class), new TokenizeMapper());
		// We'll use a TupleOutputFormat with the same schema than the intermediate schema
		mr.setTupleOutput(new Path(args[1]), getSchema());
		mr.setTupleReducer(new CountReducer());
		mr.setTupleCombiner(new CountReducer());

		mr.createJob().waitForCompletion(true);

		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AvroCustomSerializationJob(), args);
	}
}
