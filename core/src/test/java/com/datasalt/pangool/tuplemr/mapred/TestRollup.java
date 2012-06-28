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

package com.datasalt.pangool.tuplemr.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
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
import com.datasalt.pangool.tuplemr.TupleRollupReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.tuplemr.serialization.AvroFieldSerialization.AvroFieldDeserializer;
import com.datasalt.pangool.tuplemr.serialization.AvroFieldSerialization.AvroFieldSerializer;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestRollup extends AbstractHadoopTestLibrary {

	public static final String TEST_OUT = "TEST-OUTPUT";
	public static final org.apache.avro.Schema AVRO_SCHEMA;
	static {
		AVRO_SCHEMA =  org.apache.avro.Schema.createRecord("MyRecordSchema",null,null,false);
		List<org.apache.avro.Schema.Field> avroFields = new ArrayList<org.apache.avro.Schema.Field>();
		avroFields.add(new org.apache.avro.Schema.Field
				("my_int",org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),null,null));
		avroFields.add(new org.apache.avro.Schema.Field
				("my_string",org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),null,null));	
		AVRO_SCHEMA.setFields(avroFields);
		
	}
	
	
	private static class Map extends TupleMapper<Text, NullWritable> {

		private Schema schema;
		
		/**
		 * Called once at the start of the task. Override it to implement your custom logic.
		 */
		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			this.schema = context.getTupleMRConfig().getIntermediateSchema(0);
		}
		
		/**
     * 
     */
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Text key, NullWritable value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			Tuple outputKey = createTuple(key.toString(),schema);
			collector.write(outputKey);
		}
	}

	private static class IdentityRed extends TupleRollupReducer<Text, Text> {
		/**
     * 
     */
		private static final long serialVersionUID = 1L;

		private transient Text outputKey;
		private transient Text outputValue;

		@Override
		public void setup(TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {
			outputKey = new Text();
			outputValue = new Text();
		}

		@Override
		public void cleanup(TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {
		}

		@Override
		public void onOpenGroup(int depth, String field, ITuple firstElement, TupleMRContext context,
		    Collector collector) throws IOException, InterruptedException {
			outputKey.set("OPEN " + depth);
			outputValue.set(firstElement.toString());
			collector.write(outputKey, outputValue);
			System.out.println(outputKey + " => " + outputValue);
		}

		@Override
		public void onCloseGroup(int depth, String field, ITuple lastElement, TupleMRContext context,
		    Collector collector) throws IOException, InterruptedException {
			outputKey.set("CLOSE " + depth);
			outputValue.set(lastElement.toString());
			collector.write(outputKey, outputValue);
			System.out.println(outputKey + " => " + outputValue);
		}

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
		    Collector collector) throws IOException, InterruptedException {
			Iterator<ITuple> iterator = tuples.iterator();
			outputKey.set("ELEMENT");
			while(iterator.hasNext()) {
				ITuple tuple = iterator.next();
				outputValue.set(tuple.toString());
				collector.write(outputKey, outputValue);
				System.out.println(outputKey + " => " + outputValue);
			}
		}
	}

	private static Tuple createTuple(String text,Schema schema) {
		Tuple tuple = new Tuple(schema);
		String[] tokens = text.split("\\s+");
		String country = tokens[0];
		Integer age = Integer.parseInt(tokens[1]);
		String name = tokens[2];
		Integer height = Integer.parseInt(tokens[3]);

		tuple.set(0,country);
		tuple.set(1, age);
		tuple.set(2, name);
		tuple.set(3, height);
		return tuple;
	}

	@Test
	public void test1() throws IOException, InterruptedException, ClassNotFoundException, InstantiationException,
	    IllegalAccessException, TupleMRException {

		String input = TEST_OUT + "/input";
		String output = TEST_OUT + "/output";

		String[] inputElements = new String[] { 
				"ES 20 listo 250", 
				"US 14 beber 202", 
				"US 14 perro 180", 
				"US 14 perro 170",
		    "US 15 jauja 160", 
		    "US 16 listo 160", 
		    "XE 20 listo 230" 
		    };

		Schema schema = new Schema("schema",Fields.parse("country:string, age:int, name:string, height:int"));
		ITuple[] tuples = new ITuple[inputElements.length];
		int i = 0;
		for(String inputElement : inputElements) {
			withInput(input, writable(inputElement));
			tuples[i++] = createTuple(inputElement,schema);
		}
		Path outputPath = new Path(output);

		TupleMRBuilder builder = new TupleMRBuilder(getConf());
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("country","age","name");
		builder.setOrderBy(new OrderBy().add("country",Order.ASC).add("age",Order.ASC).add("name",Order.ASC));
		builder.setRollupFrom("country");
		builder.setTupleReducer(new IdentityRed());
		builder.setOutput(outputPath, new HadoopOutputFormat(SequenceFileOutputFormat.class), Text.class, Text.class);
		builder.addInput(new Path(input), new HadoopInputFormat(SequenceFileInputFormat.class), new Map());

		Job job = builder.createJob();
		job.setNumReduceTasks(1);

		assertRun(job);

		FileSystem fs = FileSystem.get(getConf());
		Path outputFile = new Path(output + "/part-r-00000");
		checkRollupOutput(outputFile, 0, 2);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, outputFile, getConf());

		assertOutput(reader, "OPEN 0", tuples[0]);
		assertOutput(reader, "OPEN 1", tuples[0]);
		assertOutput(reader, "OPEN 2", tuples[0]);
		assertOutput(reader, "ELEMENT", tuples[0]);
		assertOutput(reader, "CLOSE 2", tuples[0]);
		assertOutput(reader, "CLOSE 1", tuples[0]);
		assertOutput(reader, "CLOSE 0", tuples[0]);

		assertOutput(reader, "OPEN 0", tuples[1]);
		assertOutput(reader, "OPEN 1", tuples[1]);
		assertOutput(reader, "OPEN 2", tuples[1]);
		assertOutput(reader, "ELEMENT", tuples[1]);
		assertOutput(reader, "CLOSE 2", tuples[1]);

		assertOutput(reader, "OPEN 2", tuples[2]);
		assertOutput(reader, "ELEMENT", tuples[2]);
		assertOutput(reader, "ELEMENT", tuples[3]);
		assertOutput(reader, "CLOSE 2", tuples[3]);
		assertOutput(reader, "CLOSE 1", tuples[3]);

		assertOutput(reader, "OPEN 1", tuples[4]);
		assertOutput(reader, "OPEN 2", tuples[4]);
		assertOutput(reader, "ELEMENT", tuples[4]);
		assertOutput(reader, "CLOSE 2", tuples[4]);
		assertOutput(reader, "CLOSE 1", tuples[4]);

		assertOutput(reader, "OPEN 1", tuples[5]);
		assertOutput(reader, "OPEN 2", tuples[5]);
		assertOutput(reader, "ELEMENT", tuples[5]);
		assertOutput(reader, "CLOSE 2", tuples[5]);
		assertOutput(reader, "CLOSE 1", tuples[5]);
		assertOutput(reader, "CLOSE 0", tuples[5]);

		assertOutput(reader, "OPEN 0", tuples[6]);
		assertOutput(reader, "OPEN 1", tuples[6]);
		assertOutput(reader, "OPEN 2", tuples[6]);
		assertOutput(reader, "ELEMENT", tuples[6]);
		assertOutput(reader, "CLOSE 2", tuples[6]);
		assertOutput(reader, "CLOSE 1", tuples[6]);
		assertOutput(reader, "CLOSE 0", tuples[6]);

		cleanUp();
		trash(TEST_OUT);
	}
	
	@Test
	public void test2() throws IOException, InterruptedException, ClassNotFoundException, InstantiationException,
	    IllegalAccessException, TupleMRException {

		String input = TEST_OUT + "/input";
		String output = TEST_OUT + "/output";

		String[] inputElements = new String[] { 
				"ES 20 listo 250", 
				"US 14 beber 202", 
				"US 14 perro 180", 
				"US 14 perro 170",
		    "US 15 jauja 160", 
		    "US 16 listo 160", 
		    "XE 16 listo 230" 
		    };

		Schema schema = new Schema("schema",Fields.parse("country:string, age:int, name:string, height:int"));
		ITuple[] tuples = new ITuple[inputElements.length];
		int i = 0;
		for(String inputElement : inputElements) {
			withInput(input, writable(inputElement));
			tuples[i++] = createTuple(inputElement,schema);
		}
		Path outputPath = new Path(output);

		TupleMRBuilder builder = new TupleMRBuilder(getConf());
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("age","name","country");
		builder.setOrderBy(new OrderBy().add("country",Order.ASC).add("age",Order.ASC).add("name",Order.ASC));
		builder.setRollupFrom("age");
		builder.setTupleReducer(new IdentityRed());
		builder.setOutput(outputPath, new HadoopOutputFormat(SequenceFileOutputFormat.class), Text.class, Text.class);
		builder.addInput(new Path(input), new HadoopInputFormat(SequenceFileInputFormat.class), new Map());

		Job job = builder.createJob();
		job.setNumReduceTasks(1);

		assertRun(job);

		FileSystem fs = FileSystem.get(getConf());
		Path outputFile = new Path(output + "/part-r-00000");
		checkRollupOutput(outputFile, 1, 2);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, outputFile, getConf());

		assertOutput(reader, "OPEN 1", tuples[0]);
		assertOutput(reader, "OPEN 2", tuples[0]);
		assertOutput(reader, "ELEMENT", tuples[0]);
		assertOutput(reader, "CLOSE 2", tuples[0]);
		assertOutput(reader, "CLOSE 1", tuples[0]);

		assertOutput(reader, "OPEN 1", tuples[1]);
		assertOutput(reader, "OPEN 2", tuples[1]);
		assertOutput(reader, "ELEMENT", tuples[1]);
		assertOutput(reader, "CLOSE 2", tuples[1]);

		assertOutput(reader, "OPEN 2", tuples[2]);
		assertOutput(reader, "ELEMENT", tuples[2]);
		assertOutput(reader, "ELEMENT", tuples[3]);
		assertOutput(reader, "CLOSE 2", tuples[3]);
		assertOutput(reader, "CLOSE 1", tuples[3]);

		assertOutput(reader, "OPEN 1", tuples[4]);
		assertOutput(reader, "OPEN 2", tuples[4]);
		assertOutput(reader, "ELEMENT", tuples[4]);
		assertOutput(reader, "CLOSE 2", tuples[4]);
		assertOutput(reader, "CLOSE 1", tuples[4]);

		assertOutput(reader, "OPEN 1", tuples[5]);
		assertOutput(reader, "OPEN 2", tuples[5]);
		assertOutput(reader, "ELEMENT", tuples[5]);
		assertOutput(reader, "CLOSE 2", tuples[5]);
		assertOutput(reader, "CLOSE 1", tuples[5]);

		assertOutput(reader, "OPEN 1", tuples[6]);
		assertOutput(reader, "OPEN 2", tuples[6]);
		assertOutput(reader, "ELEMENT", tuples[6]);
		assertOutput(reader, "CLOSE 2", tuples[6]);
		assertOutput(reader, "CLOSE 1", tuples[6]);

		cleanUp();
		trash(TEST_OUT);
	}
	

	private enum State {
		OPEN, CLOSE, ELEMENT
	}

	/**
	 * 
	 * Checks that {@link RollupReducer} calls properly {@link TupleReducer#onOpenGroup}, {@link TupleReducer#onCloseGroup} and
	 * {@link TupleReducer#onGroupElements} and checks that the elements (tuples) passed are coherent. This method assumes
	 * an specific output from the {@link TupleReducer}. The output needs to be a Text,Text for key and value This will be
	 * the format used : key("OPEN depth"), value("serialized value") key("CLOSE depth"), value("serialized value")
	 * key("ELEMENT"),value("serialized element") (for every element received in onElements needs to contain a record like
	 * this)
	 * 
	 * For instance : key("OPEN 0"), value(" element1") key("OPEN 1"), value("element1 ") key("ELEMENT") , value
	 * ("element1") key("ELEMENT"),value ("element2") key("CLOSE 1"),value ("element2") key("CLOSE 0"),value("element2")
	 * 
	 * 
	 */
	public void checkRollupOutput(Path path, int minDepth, int maxDepth) throws IOException {
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.getLocal(getConf()), path, getConf());

		Text actualKey = new Text();
		Text actualValue = new Text();
		reader.next(actualKey, actualValue); // first action
		String currentKey = actualKey.toString();
		String currentValue = actualValue.toString();

		Assert.assertTrue("First output needs to be an OPEN ", currentKey.startsWith("OPEN"));
		int currentDepth = Integer.parseInt(currentKey.split(" ")[1]);
		Assert.assertEquals("First OPEN needs to match minDepth", minDepth, currentDepth);
		int lastDepth = currentDepth;
		String lastValue = currentValue;
		State lastState = State.OPEN;

		while(reader.next(actualKey, actualValue)) {
			currentKey = actualKey.toString();
			currentValue = actualValue.toString();
			if(currentKey.startsWith("OPEN")) {
				currentDepth = Integer.parseInt(currentKey.split(" ")[1]);
				Assert.assertEquals("OPEN needs to increase depth in +1 ", lastDepth + 1, currentDepth);
				Assert.assertTrue("Too many OPENs, over maxDepth ", maxDepth >= currentDepth);
				if(lastState == State.OPEN) {
					Assert.assertEquals("First element in OPEN needs to match first element in previous OPEN", lastValue,
					    currentValue);
				} else if(lastState == State.CLOSE) {
					Assert.assertNotSame("Element from new group needs to be different from last element from last group ",
					    lastValue, currentValue);
				} else {
					Assert.fail("Not allowed OPEN after ELEMENT");
				}
				lastState = State.OPEN;
				lastValue = currentValue;
				lastDepth = currentDepth;

			} else if(currentKey.startsWith("CLOSE")) {
				currentDepth = Integer.parseInt(currentKey.split(" ")[1]);
				Assert.assertNotSame("Not allowed CLOSE after OPEN , needs at least one ELEMENT in between", State.OPEN,
				    lastState);
				Assert.assertEquals("CLOSE depth needs to match previous OPEN depth", lastDepth, currentDepth);
				Assert.assertEquals("Element in CLOSE needs to match lastElement in group", lastValue, currentValue);

				lastState = State.CLOSE;
				lastValue = currentValue;
				lastDepth = currentDepth - 1;

			} else if(currentKey.startsWith("ELEMENT")) {
				Assert
				    .assertNotSame("Not allowed ELEMENT after CLOSE, needs an OPEN or ELEMENT before", State.CLOSE, lastState);
				lastState = State.ELEMENT;
				lastValue = currentValue;
			}
		}

		Assert.assertEquals("File doesn't properly finishes with a CLOSE ", State.CLOSE, lastState);
		Assert.assertEquals("Last CLOSE doesn't close the minDepth ", minDepth - 1, lastDepth);
		reader.close();
	}

	private void assertOutput(SequenceFile.Reader reader, String expectedKey, ITuple expectedValue) throws IOException {
		Text actualKey = new Text();
		Text actualValue = new Text();
		reader.next(actualKey, actualValue);

		Assert.assertEquals(new Text(expectedKey), actualKey);
		Assert.assertEquals(new Text(expectedValue.toString()), actualValue);
	}
	
	
	@SuppressWarnings("serial")
  private static class DoNothingMap extends TupleMapper<Text, NullWritable> {	

		@Override
		public void map(Text key, NullWritable value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
		}
	}

	/**
	 * Tests the case in which the reducer receives no data.  
	 */
	@Test
	public void testNoDataReducer() throws IOException, InterruptedException, ClassNotFoundException, InstantiationException,
	    IllegalAccessException, TupleMRException {

		String input = TEST_OUT + "/input";
		String output = TEST_OUT + "/output";

		String[] inputElements = new String[] { 
				"ES 20 listo 250", 
		    };

		withInput(input, writable("ES 20 listo 250"));
		
		Schema schema = new Schema("schema",Fields.parse("country:string, age:int, name:string, height:int"));
		Path outputPath = new Path(output);

		TupleMRBuilder builder = new TupleMRBuilder(getConf());
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("age","name","country");
		builder.setOrderBy(new OrderBy().add("country",Order.ASC).add("age",Order.ASC).add("name",Order.ASC));
		builder.setRollupFrom("age");
		builder.setTupleReducer(new IdentityRed());
		builder.setOutput(outputPath, new HadoopOutputFormat(SequenceFileOutputFormat.class), Text.class, Text.class);
		builder.addInput(new Path(input), new HadoopInputFormat(SequenceFileInputFormat.class), new DoNothingMap());

		Job job = builder.createJob();
		job.setNumReduceTasks(1);

		assertRun(job);
				
		cleanUp();
		trash(TEST_OUT);
	}

}
