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
package com.datasalt.pangool.tuplemr.mapred.lib.output;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.ProxyOutputFormat;
import com.datasalt.pangool.utils.CommonUtils;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestMultipleOutputs extends AbstractHadoopTestLibrary {

	public final static String INPUT = TestMultipleOutputs.class.getName() + "-input";
	public final static String OUTPUT = TestMultipleOutputs.class.getName() + "-output";

	public final static String OUTPUT_1 = "out1";
	public final static String OUTPUT_2 = "out2";
	public final static String TUPLEOUTPUT_1 = "tuple1";

	@SuppressWarnings("serial")
  public static class MyInputProcessor extends TupleMapper<LongWritable, Text> {

		private Tuple tuple;
		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
		}
		
		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			tuple.set(0, "Pere");
			tuple.set(1, 100);
			tuple.set(2, new Text("ES"));

			// We use the multiple outputs here -
			collector.write(OUTPUT_1, new Utf8((String)tuple.get(0)), new Utf8((Text)tuple.get(2)));
			collector.write(OUTPUT_2, new IntWritable((Integer)tuple.get(1)), NullWritable.get());
			collector.write(TUPLEOUTPUT_1, tuple, NullWritable.get());

			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
  public static class MyGroupHandler extends TupleReducer<DoubleWritable, NullWritable> {

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples,
		    TupleMRContext pangoolContext, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			for(ITuple tuple : tuples) {
				// We also use the multiple outputs here -
				collector.write(OUTPUT_1, tuple.get(0), tuple.get(2));
				collector.write(OUTPUT_2, new IntWritable((Integer)tuple.get(1)), NullWritable.get());
				collector.write(TUPLEOUTPUT_1, tuple, NullWritable.get());
			}

			collector.write(new DoubleWritable(1.0), NullWritable.get());
		}
	}

	@Test
	public void test() throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException, InstantiationException, IllegalAccessException {

		initHadoop();
		trash(INPUT, OUTPUT);

		// One file with one line - context will be ignored
		// Business logic in {@link MyInputProcessor}
		CommonUtils.writeTXT("ignore-me", new File(INPUT));

		TupleMRBuilder builder = new TupleMRBuilder(getConf());
		Schema baseSchema = new Schema("schema",Fields.parse("name:utf8, money:int, country:utf8"));
		builder.addIntermediateSchema(baseSchema);
		builder.setGroupByFields("country");
		builder.setOrderBy(new OrderBy().add("country",Order.ASC).add("money",Order.DESC).add("name",Order.ASC));
		builder.addInput(new Path(INPUT), new HadoopInputFormat(TextInputFormat.class), new MyInputProcessor());
		builder.setTupleReducer(new MyGroupHandler());
		builder.setOutput(new Path(OUTPUT), new HadoopOutputFormat(SequenceFileOutputFormat.class), DoubleWritable.class, NullWritable.class);
		// Configure extra outputs
		builder.addNamedOutput(OUTPUT_1, new HadoopOutputFormat(SequenceFileOutputFormat.class), Utf8.class, Utf8.class);
		builder.addNamedOutput(OUTPUT_2, new HadoopOutputFormat(SequenceFileOutputFormat.class), IntWritable.class, NullWritable.class);
		builder.addNamedTupleOutput(TUPLEOUTPUT_1, baseSchema);

		getConf()
		    .setClass(ProxyOutputFormat.PROXIED_OUTPUT_FORMAT_CONF, SequenceFileOutputFormat.class, OutputFormat.class);
		Job job = builder.createJob();
		job.waitForCompletion(true);

		// Check outputs

		withOutput(firstReducerOutput(OUTPUT), new DoubleWritable(1.0), NullWritable.get());
		withOutput(firstReducerOutput(OUTPUT + "/" + OUTPUT_1), new Text("Pere"), new Text("ES"));
		withOutput(firstMapOutput(OUTPUT + "/" + OUTPUT_1), new Text("Pere"), new Text("ES"));
		withOutput(firstReducerOutput(OUTPUT + "/" + OUTPUT_2), new IntWritable(100), NullWritable.get());
		withOutput(firstMapOutput(OUTPUT + "/" + OUTPUT_2), new IntWritable(100), NullWritable.get());

		Tuple tuple = new Tuple(baseSchema);
		tuple.set(0, "Pere");
		tuple.set(1, 100);
		tuple.set(2, "ES");
		
		withTupleOutput(firstMapOutput(OUTPUT + "/" + TUPLEOUTPUT_1), tuple);
		withTupleOutput(firstReducerOutput(OUTPUT + "/" + TUPLEOUTPUT_1), tuple);
		
		trash(INPUT, OUTPUT);
		cleanUp();
	}
}
