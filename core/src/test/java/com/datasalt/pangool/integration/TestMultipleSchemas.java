package com.datasalt.pangool.integration;

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

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Fields;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.commons.CommonUtils;
import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.test.AbstractHadoopTestLibrary;

public class TestMultipleSchemas extends AbstractHadoopTestLibrary {

	@SuppressWarnings("serial")
  public static class FirstInputProcessor extends InputProcessor<LongWritable, Text> {

		private Tuple user,country;
		
		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			Schema peopleSchema = context.getCoGrouperConfig().getSourceSchema("user");
			Schema countrySchema = context.getCoGrouperConfig().getSourceSchema("country");
			user = new Tuple(peopleSchema);
			country = new Tuple(countrySchema);
		}
		
		
		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			user.set("name", "Pere");
			user.set("money", 100);
			user.set("country", "ES");
			collector.write(user);

			user.set("name", "Iván");
			user.set("country","ES");
			user.set("money", 50);
			collector.write(user);

			user.set("country","FR");
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
  public static class MyGroupHandler extends GroupHandler<Object, Object> {

		private boolean FR_PRESENT = false;
		private boolean ES_PRESENT = false;
		private Map<String, List<String>> records = new HashMap<String, List<String>>();
		
		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, 
		    Collector collector) throws IOException, InterruptedException, CoGrouperException {
			
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
		
		public void cleanup(CoGrouperContext coGrouperContext, Collector collector) throws IOException ,InterruptedException ,CoGrouperException {
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
			Assert.assertTrue(frTuples.get(0).contains("Eric") && frTuples.get(0).contains("150"));
			Assert.assertTrue(frTuples.get(1).contains("1500"));
			Assert.assertTrue(esTuples.get(0).contains("Iván") && esTuples.get(0).contains("50"));
			Assert.assertTrue(esTuples.get(1).contains("Pere") && esTuples.get(1).contains("100"));
			Assert.assertTrue(esTuples.get(2).contains("1000"));
		};
	}

	@Test
	public void test() throws CoGrouperException, InvalidFieldException, IOException, InterruptedException,
	    ClassNotFoundException {
		CommonUtils.writeTXT("foo", new File("test-input"));
		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-output"));

		CoGrouper grouper = new CoGrouper(new Configuration());
		grouper.addSourceSchema(new Schema("user",Fields.parse("name:string, money:int, country:string")));
		grouper.addSourceSchema(new Schema("country",Fields.parse("country:string, averageSalary:int")));
		grouper.setGroupByFields("country");
		grouper.setOrderBy(new SortBy().add("country",Order.ASC));
		grouper.setSecondaryOrderBy("user", new SortBy().add("money",Order.ASC));
		
		grouper.addInput(new Path("test-input"), TextInputFormat.class, new FirstInputProcessor());
		grouper.setGroupHandler(new MyGroupHandler());
		grouper.setOutput(new Path("test-output"), TextOutputFormat.class, NullWritable.class, NullWritable.class);
		
		Job job = grouper.createJob();
		job.waitForCompletion(true);

		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-output"));
		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-input"));
	}
}
