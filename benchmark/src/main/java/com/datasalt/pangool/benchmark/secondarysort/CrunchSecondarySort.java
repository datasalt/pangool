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
package com.datasalt.pangool.benchmark.secondarysort;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.GroupingOptions;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.type.avro.Avros;
import com.cloudera.crunch.type.writable.Writables;
import com.datasalt.pangool.benchmark.AvroGroupComparator;

/**
 * Code for solving a secondary sort problem with Cascading.
 * <p>
 * The secondary sort problem is: We have a file with sales registers: {departmentId nameId timestamp saleValue}. We
 * want to obtain meaningful statistics grouping by all people who perform sales (departmentId+nameId). We want to
 * obtain total sales value for certain periods of time, therefore we need to registers in each group to come sorted by
 * "timestamp".
 */
@SuppressWarnings({ "serial" })
public class CrunchSecondarySort extends Configured implements Tool, Serializable {

	public final static class Schemas implements Serializable {
		final static Schema initialSchema;
		final static Schema groupSchema;

		static {
			List<Field> fields = new ArrayList<Field>();
			fields.add(new Field("intField", Schema.create(Schema.Type.INT), null, null));
			fields.add(new Field("strField", Schema.create(Schema.Type.STRING), null, null));
			fields.add(new Field("longField", Schema.create(Schema.Type.LONG), null, null));
			initialSchema = Schema.createRecord("Tuple3IntStringLong", null, null, false);
			initialSchema.setFields(fields);
			fields.clear();
			fields.add(new Field("intField", Schema.create(Schema.Type.INT), null, null));
			fields.add(new Field("strField", Schema.create(Schema.Type.STRING), null, null));
			groupSchema = Schema.createRecord("IntStringGroup", null, null, false);
			groupSchema.setFields(fields);
		}
	}

	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println();
			System.err.println("Two and only two arguments are accepted.");
			System.err.println("Usage: [generic options] input output");
			System.err.println();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			return 1;
		}
		// Create an object to coordinate pipeline creation and execution.
		Configuration conf = getConf();
		// Set the Avro schema for group comparator
		conf.set(AvroGroupComparator.GROUP_SCHEMA, Schemas.groupSchema.toString());
		Pipeline pipeline = new MRPipeline(CrunchSecondarySort.class, conf);
		// Reference a given text file as a collection of Strings.
		PCollection<String> lines = pipeline.readTextFile(args[0]);

		DoFn<String, Pair<GenericData.Record, Double>> doFn = new DoFn<String, Pair<GenericData.Record, Double>>() {

			GenericData.Record record;

			@Override
			public void process(String arg0, Emitter<Pair<GenericData.Record, Double>> arg1) {
				if(record == null) {
					 record = new GenericData.Record(Schemas.initialSchema);
				}
				String[] fields = arg0.split("\t");
				record.put(0, Integer.parseInt(fields[0]));
				record.put(1, fields[1]);
				record.put(2, Long.parseLong(fields[2]));
				arg1.emit(Pair.of(record, Double.parseDouble(fields[3])));
			}
		};

		DoFn<Pair<GenericData.Record, Iterable<Double>>, String> reducer = new DoFn<Pair<GenericData.Record, Iterable<Double>>, String>() {

			@Override
			public void process(Pair<GenericData.Record, Iterable<Double>> pair, Emitter<String> emitter) {

				double finalValue = 0.0;
				for(Double doubleValue : pair.second()) {
					finalValue += doubleValue;
				}
				emitter.emit(pair.first().get(0) + "\t" + pair.first().get(1) + "\t" + finalValue);
			}
		};

		GroupingOptions groupingOptions = GroupingOptions.builder().groupingComparatorClass(AvroGroupComparator.class)
		    .build();

		PCollection<String> counts = lines
		    .parallelDo(doFn, Avros.tableOf(Avros.generics(Schemas.initialSchema), Avros.doubles()))
		    .groupByKey(groupingOptions).parallelDo(reducer, Writables.strings());

		// Instruct the pipeline to write the resulting counts to a text file.
		pipeline.writeTextFile(counts, args[1]);
		// Execute the pipeline as a MapReduce.
		pipeline.done();
		return 1;
	}

	public static void main(String[] args) throws Exception {
		Logger root = Logger.getRootLogger();
		root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
		
		ToolRunner.run(new Configuration(), new CrunchSecondarySort(), args);
	}
}
