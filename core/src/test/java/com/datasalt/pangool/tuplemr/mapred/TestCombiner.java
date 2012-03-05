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
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestCombiner extends AbstractHadoopTestLibrary{


	@SuppressWarnings("serial")
	public static class Split extends TupleMapper<Text, NullWritable> {

		private Tuple tuple;
		
		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			Schema schema = context.getTupleMRConfig().getIntermediateSchema(0);
			this.tuple = new Tuple(schema);
			tuple.set("count", 1);
		}
		
		@Override
		public void map(Text key, NullWritable value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(key.toString());
			while(itr.hasMoreTokens()) {
				tuple.set("word", itr.nextToken());
				collector.write(tuple);
			}
		}
	}

	@SuppressWarnings("serial")
	public static class CountCombiner extends TupleReducer<ITuple, NullWritable> {

		private Tuple tuple;
		
		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			Schema schema = context.getTupleMRConfig().getIntermediateSchema("schema");
			this.tuple = new Tuple(schema);
		}

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			int count = 0;
			tuple.set("word", group.get("word"));
			for(ITuple tuple : tuples) {
				count += (Integer) tuple.get(1);
			}
			tuple.set("count", count);
			collector.write(this.tuple, NullWritable.get());
		}
	}

	@SuppressWarnings("serial")
	public static class Count extends TupleReducer<Utf8, IntWritable> {

		private IntWritable countToEmit;
		
		public void setup(TupleMRContext tupleMRContext, Collector collector) throws IOException, InterruptedException,
		    TupleMRException {
			countToEmit = new IntWritable();
		};

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			Iterator<ITuple> iterator = tuples.iterator();
			while(iterator.hasNext()){
				ITuple tuple = iterator.next();
				Utf8 text = (Utf8)tuple.get("word");
				countToEmit.set((Integer)tuple.get("count"));
				collector.write(text, countToEmit);
				Assert.assertFalse(iterator.hasNext());
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output) throws TupleMRException,
	    IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("word",Type.STRING));
		fields.add(Field.create("count",Type.INT));
		
		TupleMRBuilder cg = new TupleMRBuilder(conf);
		cg.addIntermediateSchema(new Schema("schema",fields));
		cg.setJarByClass(TestCombiner.class);
		cg.addInput(new Path(input), new HadoopInputFormat(SequenceFileInputFormat.class), new Split());
		cg.setOutput(new Path(output), new HadoopOutputFormat(SequenceFileOutputFormat.class), Utf8.class, IntWritable.class);
		cg.setGroupByFields("word");
		cg.setOrderBy(new OrderBy().add("word",Order.ASC));
		cg.setTupleReducer(new Count());
		cg.setTupleCombiner(new CountCombiner());

		return cg.createJob();
	}
	
	@Test
	public void test() throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException {
		
		
		Configuration conf = new Configuration();
		String input = "combiner-input";
		String output ="combiner-output";
		
		withInput(input,writable("hola don pepito hola don jose"));

		Job job = new TestCombiner().getJob(conf,input,output);
		job.setNumReduceTasks(1);
		assertRun(job);
		
		withOutput(output + "/part-r-00000",writable("don"),writable(2));
		withOutput(output+ "/part-r-00000",writable("hola"),writable(2));
		withOutput(output+ "/part-r-00000",writable("jose"),writable(1));
		withOutput(output+ "/part-r-00000",writable("pepito"),writable(1));
		
		trash(input);
		trash(output);
	}
}
