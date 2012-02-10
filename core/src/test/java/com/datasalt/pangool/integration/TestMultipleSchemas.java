//package com.datasalt.pangool.integration;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import junit.framework.Assert;
//
//import org.apache.avro.util.Utf8;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.junit.Test;
//
//import com.datasalt.pangool.CoGrouper;
//import com.datasalt.pangool.CoGrouperConfig;
//import com.datasalt.pangool.CoGrouperConfigBuilder;
//import com.datasalt.pangool.CoGrouperException;
//import com.datasalt.pangool.Schema;
//import com.datasalt.pangool.SortCriteria.SortOrder;
//import com.datasalt.pangool.SortingBuilder;
//import com.datasalt.pangool.api.GroupHandler;
//import com.datasalt.pangool.api.InputProcessor;
//import com.datasalt.pangool.commons.CommonUtils;
//import com.datasalt.pangool.commons.HadoopUtils;
//import com.datasalt.pangool.io.tuple.ITuple;
//import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
//import com.datasalt.pangool.io.tuple.Tuple;
//import com.datasalt.pangool.test.AbstractHadoopTestLibrary;
//
//public class TestMultipleSchemas extends AbstractHadoopTestLibrary {
//
//	public final static int SOURCE1 = 0;
//	public final static int SOURCE2 = 1;
//	
//	@SuppressWarnings("serial")
//  public static class FirstInputProcessor extends InputProcessor<LongWritable, Text> {
//
//		@Override
//		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
//
//			Tuple tuple = new Tuple(4);
//			tuple.setString(0, Utf8.getBytesFor("ES"));
//			tuple.setInt(1, SOURCE1);
//			tuple.setInt(2, 100);
//			tuple.setString(3, Utf8.getBytesFor("Pere"));
//
//			collector.write(tuple);
//
//			tuple.setString(0, Utf8.getBytesFor("ES"));
//			tuple.setInt(1, SOURCE1);
//			tuple.setInt(2, 50);
//			tuple.setString(3, Utf8.getBytesFor("Iván"));
//
//			collector.write(tuple);
//
//			tuple.setString(0, Utf8.getBytesFor("FR"));
//			tuple.setInt(1, SOURCE1);
//			tuple.setInt(2, 150);
//			tuple.setString(3, Utf8.getBytesFor("Eric"));
//
//			collector.write(tuple);
//
//			tuple = new Tuple(3);
//			tuple.setString(0, Utf8.getBytesFor("ES"));
//			tuple.setInt(1, SOURCE2);
//			tuple.setInt(2, 1000);
//
//			collector.write(tuple);
//
//			tuple.setString(0, Utf8.getBytesFor("FR"));
//			tuple.setInt(1, SOURCE2);
//			tuple.setInt(2, 1500);
//
//			collector.write(tuple);
//		}
//	}
//
//	@SuppressWarnings("serial")
//  public static class MyGroupHandler extends GroupHandler<Object, Object> {
//
//		private boolean FRPRESENT = false;
//		private boolean ESPRESENT = false;
//		private Map<String, List<String>> records = new HashMap<String, List<String>>();
//		
//		@Override
//		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, 
//		    Collector collector) throws IOException, InterruptedException, CoGrouperException {
//			
//			String groupString = new Utf8(group.getString(0)).toString();
//			if(groupString.equals("FR")) {
//				FRPRESENT = true;
//				if(!ESPRESENT) {
//					throw new AssertionError("ES should have come before FR");
//				}
//			} else if(groupString.equals("ES")) {
//				ESPRESENT = true;
//			}
//			for(Object tuple : tuples) {
//				List<String> savedTuples = records.get(groupString);
//				if(savedTuples == null) {
//					savedTuples = new ArrayList<String>();
//					records.put(groupString, savedTuples);
//				}
//				savedTuples.add(tuple.toString());
//			}
//		}
//		
//		public void cleanup(CoGrouperContext coGrouperContext, Collector collector) throws IOException ,InterruptedException ,CoGrouperException {
//			/*
//			 * Validate test conditions
//			 */
//			if(!ESPRESENT) {
//				throw new AssertionError("ES group not present");
//			}
//			if(!FRPRESENT) {
//				throw new AssertionError("FR group not present");
//			}
//			List<String> frTuples = records.get("FR");
//			List<String> esTuples = records.get("ES");
//			Assert.assertTrue(frTuples.get(0).contains("Eric") && frTuples.get(0).contains("150"));
//			Assert.assertTrue(frTuples.get(1).contains("1500"));
//			Assert.assertTrue(esTuples.get(0).contains("Iván") && esTuples.get(0).contains("50"));
//			Assert.assertTrue(esTuples.get(1).contains("Pere") && esTuples.get(1).contains("100"));
//			Assert.assertTrue(esTuples.get(2).contains("1000"));
//		};
//	}
//
//	@Test
//	public void test() throws CoGrouperException, InvalidFieldException, IOException, InterruptedException,
//	    ClassNotFoundException {
//		
//		CoGrouperConfig config = new CoGrouperConfigBuilder()
//		    .addSchema(0, Schema.parse("name:string, money:int, country:string"))
//		    .addSchema(1, Schema.parse("country:string, averageSalary:int"))
//		    .setGroupByFields("country")
//		    .setSorting(
//		        new SortingBuilder()
//		        .add("country")
//		        .addSourceId(SortOrder.ASC)
//		        	.secondarySort(0)
//		        	.add("money")
//		        .buildSorting())
//		    .build();
//
//		CommonUtils.writeTXT("foo", new File("test-input"));
//		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-output"));
//
//		Job job = new CoGrouper(config, getConf())
//		    .addInput(new Path("test-input"), TextInputFormat.class, new FirstInputProcessor())
//		    .setGroupHandler(new MyGroupHandler())
//		    .setOutput(new Path("test-output"), TextOutputFormat.class, NullWritable.class, NullWritable.class).createJob();
//
//		job.waitForCompletion(true);
//
//		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-output"));
//		HadoopUtils.deleteIfExists(FileSystem.get(getConf()), new Path("test-input"));
//	}
//}
