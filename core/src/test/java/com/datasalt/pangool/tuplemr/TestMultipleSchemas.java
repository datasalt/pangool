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
package com.datasalt.pangool.tuplemr;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;
import com.datasalt.pangool.utils.CommonUtils;
import com.datasalt.pangool.utils.HadoopUtils;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestMultipleSchemas extends AbstractHadoopTestLibrary {

	@SuppressWarnings("serial")
	public static class FirstInputProcessor extends TupleMapper<LongWritable, Text> {

		private Tuple user, country;

		public void setup(TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {
			Schema peopleSchema = context.getTupleMRConfig().getIntermediateSchema("user");
			Schema countrySchema = context.getTupleMRConfig().getIntermediateSchema("country");
			user = new Tuple(peopleSchema);
			country = new Tuple(countrySchema);
		}

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context,
		    Collector collector) throws IOException, InterruptedException {
			user.set("name", "Pere");
			user.set("money", 100);
			user.set("country", "ES");
			collector.write(user);

			user.set("name", "Iván");
			user.set("country", "ES");
			user.set("money", 50);
			collector.write(user);

			user.set("country", "FR");
			user.set("money", 150);
			user.set("name", "Eric");
			collector.write(user);

			country.set("country", "ES");
			country.set("averageSalary", 1000);
			collector.write(country);

			country.set("country", "FR");
			country.set("averageSalary", 1500);
			collector.write(country);
		}
	}

	@SuppressWarnings("serial")
	public static class MyGroupHandler extends TupleReducer<Object, Object> {

		private boolean FR_PRESENT = false;
		private boolean ES_PRESENT = false;
		private Map<String, List<String>> records = new HashMap<String, List<String>>();

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
		    Collector collector) throws IOException, InterruptedException, TupleMRException {

			String groupString = group.get(0).toString();
			if(groupString.equals("FR")) {
				FR_PRESENT = true;
				if(!ES_PRESENT) {
					throw new AssertionError("ES should have come before FR");
				}
			} else if(groupString.equals("ES")) {
				ES_PRESENT = true;
			}
			for(Object tuple : tuples) {
				List<String> savedTuples = records.get(groupString);
				if(savedTuples == null) {
					savedTuples = new ArrayList<String>();
					records.put(groupString, savedTuples);
				}
				savedTuples.add(tuple.toString());
			}
		}

		public void cleanup(TupleMRContext tupleMRContext, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			/*
			 * Validate test conditions
			 */
			if(!ES_PRESENT) {
				throw new AssertionError("ES group not present");
			}
			if(!FR_PRESENT) {
				throw new AssertionError("FR group not present");
			}
			List<String> frTuples = records.get("FR");
			List<String> esTuples = records.get("ES");
			Assert.assertTrue(frTuples.get(0).contains("Eric")
			    && frTuples.get(0).contains("150"));
			Assert.assertTrue(frTuples.get(1).contains("1500"));
			Assert.assertTrue(esTuples.get(0).contains("Iván")
			    && esTuples.get(0).contains("50"));
			Assert.assertTrue(esTuples.get(1).contains("Pere")
			    && esTuples.get(1).contains("100"));
			Assert.assertTrue(esTuples.get(2).contains("1000"));
		};
	}

	@Test
	public void test() throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException {
		CommonUtils.writeTXT("foo", new File("test-input"));
		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-output"));

		TupleMRBuilder builder = new TupleMRBuilder(new Configuration());
		builder.addIntermediateSchema(new Schema("country", Fields
		    .parse("country:string, averageSalary:int")));
		builder.addIntermediateSchema(new Schema("user", Fields
		    .parse("name:string, money:int, country:string")));

		builder.setGroupByFields("country");
		builder
		    .setOrderBy(new OrderBy().add("country", Order.ASC).addSchemaOrder(Order.DESC));
		builder.setSpecificOrderBy("user", new OrderBy().add("money", Order.ASC));

		builder.addInput(new Path("test-input"),
		    new HadoopInputFormat(TextInputFormat.class), new FirstInputProcessor());
		builder.setTupleReducer(new MyGroupHandler());
		builder.setOutput(new Path("test-output"), new HadoopOutputFormat(
		    TextOutputFormat.class), NullWritable.class, NullWritable.class);

		Job job = builder.createJob();
		assertRun(job);

		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-output"));
		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-input"));
	}
}
