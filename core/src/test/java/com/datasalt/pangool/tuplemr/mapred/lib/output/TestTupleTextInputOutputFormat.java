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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.MultipleOutputsCollector;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat.FieldSelector;
import com.datasalt.pangool.utils.CommonUtils;
import com.datasalt.pangool.utils.HadoopUtils;
import com.google.common.io.Files;

@SuppressWarnings({ "rawtypes", "serial" })
public class TestTupleTextInputOutputFormat extends BaseTest implements Serializable {

	public static String OUT = TestTupleTextInputOutputFormat.class.getName() + "-out";
	public static String IN = TestTupleTextInputOutputFormat.class.getName() + "-in";

	public static enum TestEnum {
		MICKEY, MOUSE, MINIE;
	}

	@Test
	public void test() throws TupleMRException, IOException, InterruptedException, ClassNotFoundException {

		String line1 = "foo1 10.0 bar1 1.0 100 1000000 true MICKEY";
		String line2 = "foo2 20.0 bar2 2.0 200 2000000 false MOUSE";
		String line3 = "foo3 30.0 bar3 3.0 300 3000000 true MINIE";

		// The input is a simple space-separated file with no quotes
		CommonUtils.writeTXT(line1 + "\n" + line2 + "\n" + line3, new File(IN));
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		HadoopUtils.deleteIfExists(fS, outPath);

		// Define the Schema according to the text file
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("strField1", Type.STRING));
		fields.add(Field.create("floatField", Type.FLOAT));
		fields.add(Field.create("strField2", Type.STRING));
		fields.add(Field.create("doubleField", Type.DOUBLE));
		fields.add(Field.create("intField", Type.INT));
		fields.add(Field.create("longField", Type.LONG));
		fields.add(Field.create("booleanField", Type.BOOLEAN));
		fields.add(Field.createEnum("enumField", TestEnum.class));

		Schema schema = new Schema("schema", fields);

		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("strField1"); // but we don't care, really
		/*
		 * Define the Input Format and the Output Format!
		 */
		InputFormat inputFormat = new TupleTextInputFormat(schema, false, false, ' ',
		    TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER,
		    FieldSelector.NONE, TupleTextInputFormat.NO_NULL_STRING);
		OutputFormat outputFormat = new TupleTextOutputFormat(schema, false, ' ',
		    TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER);

		builder.addInput(inPath, inputFormat, new IdentityTupleMapper());
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setOutput(outPath, outputFormat, ITuple.class, NullWritable.class);
		builder.createJob().waitForCompletion(true);
		Job job = builder.createJob();
		assertRun(job);

		Assert.assertEquals(line1 + "\n" + line2 + "\n" + line3,
		    Files.toString(new File(OUT + "/" + "part-r-00000"), Charset.forName("UTF-8")).trim());

		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
	}

	@Test
	public void test2() throws TupleMRException, IOException, InterruptedException, ClassNotFoundException {

		String line1 = "1,\"Kabul\",\"AFG\",\"Kabol\",1780000";
		String line2 = "2,\"Qandahar\",\"AFG\",\"Qandahar\",237500";

		String line1out = "\"1\",\"Kabul\",\"AFG\",\"Kabol\",\"1780000\"";
		String line2out = "\"2\",\"Qandahar\",\"AFG\",\"Qandahar\",\"237500\"";

		// The input is a simple space-separated file with no quotes
		CommonUtils.writeTXT(line1 + "\n" + line2, new File(IN));
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		HadoopUtils.deleteIfExists(fS, outPath);

		// Define the Schema according to the text file
		Schema schema = new Schema("schema",
		    Fields.parse("id:int,name:string,country_code:string,district:string,population:int"));

		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("id"); // but we don't care, really
		/*
		 * Define the Input Format and the Output Format!
		 */
		InputFormat inputFormat = new TupleTextInputFormat(schema, false, false, ',', '"', '\\', FieldSelector.NONE, TupleTextInputFormat.NO_NULL_STRING);
		OutputFormat outputFormat = new TupleTextOutputFormat(schema, false, ',', '"', '\\');

		builder.addInput(inPath, inputFormat, new IdentityTupleMapper());
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setOutput(outPath, outputFormat, ITuple.class, NullWritable.class);
		builder.createJob().waitForCompletion(true);
		Job job = builder.createJob();
		assertRun(job);

		Assert.assertEquals(line1out + "\n" + line2out,
		    Files.toString(new File(OUT + "/" + "part-r-00000"), Charset.forName("UTF-8")).trim());

		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
	}

	@Test
	public void testHeader() throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException {

		String line1 = "foo1 10.0 bar1 1.0 100 1000000 true MICKEY";
		String line2 = "foo2 20.0 bar2 2.0 200 2000000 false MOUSE";
		String line3 = "foo3 30.0 bar3 3.0 300 3000000 true MINIE";

		String outHeader = "strField1 floatField strField2 doubleField intField longField booleanField enumField";

		// The input is a simple space-separated file with no quotes
		CommonUtils.writeTXT(line1 + "\n" + line2 + "\n" + line3, new File(IN));
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		HadoopUtils.deleteIfExists(fS, outPath);

		// Define the Schema according to the text file
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("strField1", Type.STRING));
		fields.add(Field.create("floatField", Type.FLOAT));
		fields.add(Field.create("strField2", Type.STRING));
		fields.add(Field.create("doubleField", Type.DOUBLE));
		fields.add(Field.create("intField", Type.INT));
		fields.add(Field.create("longField", Type.LONG));
		fields.add(Field.create("booleanField", Type.BOOLEAN));
		fields.add(Field.createEnum("enumField", TestEnum.class));

		Schema schema = new Schema("schema", fields);

		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("strField1"); // but we don't care, really
		/*
		 * Define the Input Format and the Output Format!
		 */
		InputFormat inputFormat = new TupleTextInputFormat(schema, true, false, ' ',
		    TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER,
		    FieldSelector.NONE, TupleTextInputFormat.NO_NULL_STRING);
		OutputFormat outputFormat = new TupleTextOutputFormat(schema, true, ' ',
		    TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER);

		builder.addInput(inPath, inputFormat, new IdentityTupleMapper());
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setOutput(outPath, outputFormat, ITuple.class, NullWritable.class);
		builder.createJob().waitForCompletion(true);
		Job job = builder.createJob();
		assertRun(job);

		Assert.assertEquals(outHeader + "\n" + line2 + "\n" + line3,
		    Files.toString(new File(OUT + "/" + "part-r-00000"), Charset.forName("UTF-8")).trim());

		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
	}

	@Test
	public void testNulls() throws IOException, InterruptedException, ClassNotFoundException, TupleMRException, URISyntaxException {
		
		String line1 = "\"Joe\",\\N,,\"\\\"Joan\\\"\",\"\"";

		CommonUtils.writeTXT(line1, new File(IN));
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		HadoopUtils.deleteIfExists(fS, outPath);

		Schema schema = new Schema(
		    "schema",
		    Fields
		        .parse("name:string,name2:string,age:int,name3:string,emptystring:string"));

		MapOnlyJobBuilder mO = new MapOnlyJobBuilder(conf);
		mO.addInput(inPath, new TupleTextInputFormat(schema, false, true, ',', '"',
		    '\\', FieldSelector.NONE, TupleTextInputFormat.NO_NULL_STRING),
		    new MapOnlyMapper<ITuple, NullWritable, NullWritable, NullWritable>() {

			    protected void map(ITuple key, NullWritable value, Context context,
			        MultipleOutputsCollector collector) throws IOException, InterruptedException {
			    	
			    	try {
				    	Assert.assertNull(key.get("name2"));
				    	Assert.assertNull(key.get("age"));
				    	Assert.assertEquals("Joe", key.get("name"));
				    	Assert.assertEquals("\"Joan\"", key.get("name3"));
				    	Assert.assertEquals("", key.get("emptystring"));
			    	} catch(Throwable t) {
			    		t.printStackTrace();
			    		throw new RuntimeException(t);
			    	}
			    }
		    });
		
		mO.setOutput(outPath, new HadoopOutputFormat(NullOutputFormat.class), NullWritable.class,
		    NullWritable.class);
		Job job = mO.createJob();
		assertTrue(job.waitForCompletion(true));

		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
	}
	
	@Test
	public void testQuotes() throws IOException, InterruptedException, ClassNotFoundException,
	    TupleMRException, URISyntaxException {
		
		String line1 = "\"MYS\",\"Malaysia\",\"Asia\",\"Southeast Asia\",329758.00,1957,22244000,70.8,69213.00,97884.00,\"Malaysia\",\"Constitutional Monarchy, Federation\",\"Salahuddin Abdul Aziz Shah Alhaj\",2464,\"MY\"";

		CommonUtils.writeTXT(line1, new File(IN));
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		HadoopUtils.deleteIfExists(fS, outPath);

		Schema schema = new Schema(
		    "schema",
		    Fields
		        .parse("code:string," +
		        		"name:string," +
		        		"continent:string," +
		        		"region:string," +
		        		"surface_area:double," +
		        		"indep_year:int," +
		        		"population:int," +
		        		"life_expectancy:double," +
		        		"gnp:double," +
		        		"gnp_old:double," +
		        		"local_name:string," +
		        		"government_form:string," +
		        		"head_of_state:string," +
		        		"capital:int," +
		        		"code2:string"));

		
		MapOnlyJobBuilder mO = new MapOnlyJobBuilder(conf);
		mO.addInput(inPath, new TupleTextInputFormat(schema, false, false, ',', '"',
		    '\\', FieldSelector.NONE, TupleTextInputFormat.NO_NULL_STRING),
		    new MapOnlyMapper<ITuple, NullWritable, NullWritable, NullWritable>() {

			    protected void map(ITuple key, NullWritable value, Context context,
			        MultipleOutputsCollector collector) throws IOException, InterruptedException {
			    	
			    	try {
				    	Assert.assertEquals("Constitutional Monarchy, Federation", key.get("government_form").toString());
				    	Assert.assertEquals("Salahuddin Abdul Aziz Shah Alhaj", key.get("head_of_state").toString());
				    	Assert.assertEquals(2464, key.get("capital"));
			    	} catch(Throwable t) {
			    		t.printStackTrace();
			    		throw new RuntimeException(t);
			    	}
			    }
		    });
		mO.setOutput(outPath, new HadoopOutputFormat(NullOutputFormat.class), NullWritable.class,
		    NullWritable.class);
		Job job = mO.createJob();
		assertTrue(job.waitForCompletion(true));
		
		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
	}

	@Test
	public void testFieldSelection() throws IOException, TupleMRException, InterruptedException,
	    ClassNotFoundException {
		String line1 = "foo1 10.0 bar1 1.0 100 1000000 true MICKEY";
		String line2 = "foo2 20.0 bar2 2.0 200 2000000 false MOUSE";
		String line3 = "foo3 30.0 bar3 3.0 300 3000000 true MINIE";

		// The input is a simple space-separated file with no quotes
		CommonUtils.writeTXT(line1 + "\n" + line2 + "\n" + line3, new File(IN));
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		HadoopUtils.deleteIfExists(fS, outPath);

		// Define the Schema according to the text file
		// We will only select a subset of the file columns
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("floatField", Type.FLOAT));
		fields.add(Field.create("intField", Type.INT));
		fields.add(Field.create("booleanField", Type.BOOLEAN));

		Schema schema = new Schema("schema", fields);

		// Define a FieldSelector to select only columns 1, 4, 6
		// 0 is the first column
		FieldSelector selector = new FieldSelector(1, 4, 6);

		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("floatField"); // but we don't care, really
		// Define the Input Format and the Output Format!
		// Add the selector to the input format
		InputFormat inputFormat = new TupleTextInputFormat(schema, false, false, ' ',
		    TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER, selector, TupleTextInputFormat.NO_NULL_STRING);
		OutputFormat outputFormat = new TupleTextOutputFormat(schema, false, ' ',
		    TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER);

		builder.addInput(inPath, inputFormat, new IdentityTupleMapper());
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setOutput(outPath, outputFormat, ITuple.class, NullWritable.class);
		builder.createJob().waitForCompletion(true);
		Job job = builder.createJob();
		assertRun(job);

		// This is what we expect as output after field selection
		line1 = "10.0 100 true";
		line2 = "20.0 200 false";
		line3 = "30.0 300 true";

		Assert.assertEquals(line1 + "\n" + line2 + "\n" + line3,
		    Files.toString(new File(OUT + "/" + "part-r-00000"), Charset.forName("UTF-8")).trim());

		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
	}
}
