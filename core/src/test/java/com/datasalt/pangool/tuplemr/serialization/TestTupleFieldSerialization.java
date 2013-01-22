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
package com.datasalt.pangool.tuplemr.serialization;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
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
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

/**
 * This unit test checks that it is possible to serialize a Tuple inside a Tuple by using stateful serialization
 * {@link TupleFieldSerialization}. It performs a kind of join where two different data sources with no common fields
 * at all are joined by a new field (partitionId).
 */
@SuppressWarnings("serial")
public class TestTupleFieldSerialization extends AbstractHadoopTestLibrary implements Serializable {

	public final static String INPUT1 = "in-1-" + TestTupleFieldSerialization.class.getName();
	public final static String INPUT2 = "in-2-" + TestTupleFieldSerialization.class.getName();
	public final static String OUTPUT = "out-" + TestTupleFieldSerialization.class.getName();

	@Test
	public void test() throws Exception {
		initHadoop();
		trash(INPUT1, INPUT2, OUTPUT);

		// Prepare input
		BufferedWriter writer;

		// INPUT1
		writer = new BufferedWriter(new FileWriter(INPUT1));
		writer.write("foo1" + "\t" + "30" + "\n");
		writer.write("foo2" + "\t" + "20" + "\n");
		writer.write("foo3" + "\t" + "140" + "\n");
		writer.write("foo4" + "\t" + "110" + "\n");
		writer.write("foo5" + "\t" + "220" + "\n");
		writer.write("foo6" + "\t" + "260" + "\n");
		writer.close();

		// INPUT2
		writer = new BufferedWriter(new FileWriter(INPUT2));
		writer.write("4.5" + "\t" + "true" + "\n");
		writer.write("4.6" + "\t" + "false" + "\n");
		writer.close();

		TupleMRBuilder builder = new TupleMRBuilder(getConf());

		final Schema tupleSchema1 = new Schema("tupleSchema1", Fields.parse("a:string, b:int"));
		final Schema tupleSchema2 = new Schema("tupleSchema2", Fields.parse("c:double, d:boolean"));

		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("partitionId", Type.INT));
		fields.add(Fields.createTupleField("tuple1", tupleSchema1));
		final Schema schema1 = new Schema("tupleInTuple1", fields);

		fields.clear();
		fields.add(Field.create("partitionId", Type.INT));
		fields.add(Fields.createTupleField("tuple2", tupleSchema2));
		final Schema schema2 = new Schema("tupleInTuple2", fields);

		builder.addIntermediateSchema(schema1);
		builder.addIntermediateSchema(schema2);

		builder.addInput(new Path(INPUT1), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tupleInTuple1 = new Tuple(schema1);
			    ITuple tuple1 = new Tuple(tupleSchema1);

			    @Override
			    public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {
				    String[] split = value.toString().split("\t");
				    tuple1.set("a", split[0]);
				    tuple1.set("b", Integer.parseInt(split[1]));

				    tupleInTuple1.set("partitionId", 0);
				    tupleInTuple1.set("tuple1", tuple1);
				    collector.write(tupleInTuple1);
			    }
		    });

		builder.addInput(new Path(INPUT2), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tupleInTuple2 = new Tuple(schema2);
			    ITuple tuple2 = new Tuple(tupleSchema2);

			    @Override
			    public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {
				    String[] split = value.toString().split("\t");
				    tuple2.set("c", Double.parseDouble(split[0]));
				    tuple2.set("d", Boolean.parseBoolean(split[1]));

				    tupleInTuple2.set("partitionId", 0);
				    tupleInTuple2.set("tuple2", tuple2);
				    collector.write(tupleInTuple2);
			    }
		    });

		builder.setTupleReducer(new TupleReducer<Text, NullWritable>() {

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
			    throws IOException, InterruptedException, TupleMRException {
				
				Iterator<ITuple> iterator = tuples.iterator();
				ITuple currentTuple;
				
				assertEquals(0, group.get("partitionId"));
				
				currentTuple = iterator.next();
				assertEquals("foo1", ((ITuple)currentTuple.get("tuple1")).get("a").toString());
				assertEquals(30, ((ITuple)currentTuple.get("tuple1")).get("b"));
				
				currentTuple = iterator.next();
				assertEquals("foo2", ((ITuple)currentTuple.get("tuple1")).get("a").toString());
				assertEquals(20, ((ITuple)currentTuple.get("tuple1")).get("b"));

				currentTuple = iterator.next();
				assertEquals("foo3", ((ITuple)currentTuple.get("tuple1")).get("a").toString());
				assertEquals(140, ((ITuple)currentTuple.get("tuple1")).get("b"));

				currentTuple = iterator.next();
				assertEquals("foo4", ((ITuple)currentTuple.get("tuple1")).get("a").toString());
				assertEquals(110, ((ITuple)currentTuple.get("tuple1")).get("b"));

				currentTuple = iterator.next();
				assertEquals("foo5", ((ITuple)currentTuple.get("tuple1")).get("a").toString());
				assertEquals(220, ((ITuple)currentTuple.get("tuple1")).get("b"));

				currentTuple = iterator.next();
				assertEquals("foo6", ((ITuple)currentTuple.get("tuple1")).get("a").toString());
				assertEquals(260, ((ITuple)currentTuple.get("tuple1")).get("b"));

				// Second data source BEGINS
				currentTuple = iterator.next();
				assertEquals(4.5, ((ITuple)currentTuple.get("tuple2")).get("c"));
				assertEquals(true, ((ITuple)currentTuple.get("tuple2")).get("d"));
				
				currentTuple = iterator.next();
				assertEquals(4.6, ((ITuple)currentTuple.get("tuple2")).get("c"));
				assertEquals(false, ((ITuple)currentTuple.get("tuple2")).get("d"));
			};
		});
		builder.setGroupByFields("partitionId");
		builder.setOutput(new Path(OUTPUT), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		Job job = builder.createJob();
		try {
			job.waitForCompletion(true);
		} finally {
			builder.cleanUpInstanceFiles();
		}
		trash(INPUT1, INPUT2, OUTPUT);
	}
}
