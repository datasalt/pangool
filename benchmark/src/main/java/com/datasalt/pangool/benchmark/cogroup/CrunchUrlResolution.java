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
package com.datasalt.pangool.benchmark.cogroup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
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
import com.cloudera.crunch.PGroupedTable;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.type.avro.AvroTableType;
import com.cloudera.crunch.type.avro.Avros;
import com.cloudera.crunch.type.writable.Writables;
import com.datasalt.pangool.benchmark.AvroGroupComparator;

/**
 * Code for solving the URL Resolution CoGroup Problem in Crunch.
 * <p>
 * The URL Resolution CoGroup Problem is: We have one file with URL Registers: {url timestamp ip} and another file with
 * canonical URL mapping: {url canonicalUrl}. We want to obtain the URL Registers file with the url substituted with the
 * canonical one according to the mapping file: {canonicalUrl timestamp ip}.
 */
@SuppressWarnings("serial")
public class CrunchUrlResolution extends Configured implements Tool, Serializable {

	public final static int SOURCE_URL_MAP = 0;
	public final static int SOURCE_URL_REGISTER = 1;

	public final static class Schemas implements Serializable {
		final static Schema groupSchema;
		final static Schema groupComparatorSchema;
		final static Schema particularUrlRegisterSchema;

		static {
			List<Field> fields = new ArrayList<Field>();
			fields.add(new Field("url", Schema.create(Schema.Type.STRING), null, null));
			groupComparatorSchema = Schema.createRecord("StringGroup", null, null, false);
			groupComparatorSchema.setFields(fields);
			fields.clear();
			fields.add(new Field("url", Schema.create(Schema.Type.STRING), null, null));
			fields.add(new Field("schemaId", Schema.create(Schema.Type.INT), null, null));
			groupSchema = Schema.createRecord("IntStringGroup", null, null, false);
			groupSchema.setFields(fields);
			fields.clear();
			fields.add(new Field("timestamp", Schema.create(Schema.Type.LONG), null, null));
			fields.add(new Field("ip", Schema.create(Schema.Type.STRING), null, null));
			particularUrlRegisterSchema = Schema.createRecord("crunch.LongString", null, null, false); // crunch.LongString otherwise it doesn't work -> Maybe a bug?
			particularUrlRegisterSchema.setFields(fields);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println();
			System.err.println("Three and only three arguments are accepted.");
			System.err.println("Usage: " + this.getClass().getName()
			    + " [generic options] inputUrlMap inputUrlRegister output");
			System.err.println();
			GenericOptionsParser.printGenericCommandUsage(System.err);
			return 1;
		}
		String urlMapFile = args[0];
		String urlRegisterFile = args[1];
		String output = args[2];

		Configuration conf = getConf();
		conf.set(AvroGroupComparator.GROUP_SCHEMA, Schemas.groupComparatorSchema.toString());
		Pipeline pipeline = new MRPipeline(CrunchUrlResolution.class, getConf());

		PCollection<String> urlMapLines = pipeline.readTextFile(urlMapFile);
		PCollection<String> urlRegisterLines = pipeline.readTextFile(urlRegisterFile);

		DoFn<String, Pair<Record, Pair<String, Record>>> doFnUrlMap = new DoFn<String, Pair<Record, Pair<String, Record>>>() {

			Record emptyRecord = null;
			Record group;

			@Override
			public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
				super.setContext(context);
				group = new Record(Schemas.groupSchema);
			}

			@Override
			public void process(String input, Emitter<Pair<Record, Pair<String, Record>>> emitter) {
				String[] fields = input.split("\t");

				group.put(0, fields[0]); // url
				group.put(1, SOURCE_URL_MAP); // source

				emitter.emit(Pair.of(group, Pair.of(fields[1], emptyRecord)));
			}
		};

		DoFn<String, Pair<GenericData.Record, Pair<String, Record>>> doFnUrlRegister = new DoFn<String, Pair<GenericData.Record, Pair<String, GenericData.Record>>>() {

			GenericData.Record particularRecord;
			GenericData.Record group;

			String nullString = null;

			@Override
			public void setContext(TaskInputOutputContext<?, ?, ?, ?> context) {
				super.setContext(context);
				particularRecord = new GenericData.Record(Schemas.particularUrlRegisterSchema);
				group = new GenericData.Record(Schemas.groupSchema);
			}

			@Override
			public void process(String input, Emitter<Pair<Record, Pair<String, Record>>> emitter) {
				String[] fields = input.split("\t");

				group.put(0, fields[0]); // url
				group.put(1, SOURCE_URL_REGISTER); // source

				particularRecord.put("timestamp", Long.parseLong(fields[1]));
				particularRecord.put("ip", fields[2]);

				emitter.emit(Pair.of(group, Pair.of(nullString, particularRecord)));
			}
		};

		DoFn<Pair<Record, Iterable<Pair<String, Record>>>, String> reducer = new DoFn<Pair<Record, Iterable<Pair<String, Record>>>, String>() {

			@Override
			public void process(Pair<Record, Iterable<Pair<String, Record>>> input, Emitter<String> emitter) {
				Record groupRecord = input.first();
				String cannonicalUrl = null;
				for(Pair<String, Record> pair : input.second()) {
					Record particularRecord = pair.second();
					if((Integer) groupRecord.get(1) == SOURCE_URL_MAP) {
						cannonicalUrl = pair.first();
					} else {
						emitter.emit(cannonicalUrl + "\t" + particularRecord.get(0) + "\t" + particularRecord.get(1));
					}
				}
			}
		};

		GroupingOptions groupingOptions = GroupingOptions.builder().groupingComparatorClass(AvroGroupComparator.class)
		    .build();

		AvroTableType<Record, Pair<String, Record>> tableType = Avros.tableOf(Avros.generics(Schemas.groupSchema),
		    Avros.pairs(Avros.strings(), Avros.generics(Schemas.particularUrlRegisterSchema)));

		PTable<Record, Pair<String, Record>> registerTable = urlRegisterLines.parallelDo(doFnUrlRegister, tableType);
		PTable<Record, Pair<String, Record>> urlMapTable = urlMapLines.parallelDo(doFnUrlMap, tableType);

		@SuppressWarnings("unchecked")
		PTable<Record, Pair<String, Record>> both = registerTable.union(urlMapTable);
		PGroupedTable<Record, Pair<String, Record>> grouped = both.groupByKey(groupingOptions);

		pipeline.writeTextFile(grouped.parallelDo(reducer, Writables.strings()), output);
		// Execute the pipeline as a MapReduce.
		pipeline.done();

		return 1;
	}

	public final static void main(String[] args) throws Exception {
		Logger root = Logger.getRootLogger();
		root.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));

		ToolRunner.run(new Configuration(), new CrunchUrlResolution(), args);
	}
}
