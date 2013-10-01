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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.utils.CommonUtils;
import com.datasalt.pangool.utils.HadoopUtils;
import com.datasalt.pangool.utils.TaskAttemptContextFactory;
import com.google.common.io.Files;

public class TestTupleInputOutputFormat extends BaseTest {

	private final static Log logger = LogFactory.getLog(TestTupleInputOutputFormat.class);

	public static String OUT = TestTupleInputOutputFormat.class.getName() + "-out";
	public static String OUT_TEXT = TestTupleInputOutputFormat.class.getName() + "-out-text";
	public static String IN = TestTupleInputOutputFormat.class.getName() + "-in";

	public static class MyInputProcessor extends TupleMapper<LongWritable, Text> {

		private static final long serialVersionUID = 1L;
		private Tuple tuple;

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			if(tuple == null) {
				tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
			}
			tuple.set(0, "title");
			tuple.set(1, value);
			collector.write(tuple);
		}
	}

	public static class MyGroupHandler extends TupleReducer<Text, NullWritable> {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			for(ITuple tuple : tuples) {
				collector.write((Text) tuple.get(0), NullWritable.get());
			}
		}
	}

	@Test
	public void test() throws TupleMRException, IOException, InterruptedException, ClassNotFoundException {

		CommonUtils.writeTXT("foo1 bar1\nbar2 foo2", new File(IN));
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		Path outPathText = new Path(OUT_TEXT);
		HadoopUtils.deleteIfExists(fS, outPath);
		HadoopUtils.deleteIfExists(fS, outPathText);

		Schema originalSchema = new Schema("schema", Fields.parse("title:string, content:string"));

		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addIntermediateSchema(originalSchema);
		builder.setGroupByFields("title");
		builder.setOrderBy(new OrderBy().add("title", Order.ASC).add("content", Order.ASC));

		builder.setTupleReducer(new IdentityTupleReducer());
		builder.setTupleOutput(outPath, originalSchema);
		builder.addInput(inPath, new HadoopInputFormat(TextInputFormat.class), new MyInputProcessor());

		Job job = builder.createJob();
		try {
			job.waitForCompletion(true);
		} finally {
			builder.cleanUpInstanceFiles();
		}

		// Use output as input of new TupleMRBuilder
		// To make things nicer, we evolve the Schema and use a different Schema for reading the Tuple File.
		// We remove the "content" and add a new nullable field.
		Schema evolvedSchema = new Schema("evolved", Fields.parse("content:string, new_field:string?"));

		builder = new TupleMRBuilder(conf);
		builder.addTupleInput(outPath, evolvedSchema, new IdentityTupleMapper()); 
		builder.addIntermediateSchema(evolvedSchema);
		builder.setGroupByFields("content");
		builder.setTupleReducer(new MyGroupHandler());
		builder.setOutput(outPathText, new HadoopOutputFormat(TextOutputFormat.class), Text.class,
				NullWritable.class);

		job = builder.createJob();
		try {
			assertRun(job);
		} finally {
			builder.cleanUpInstanceFiles();
		}

		Assert.assertEquals("bar2 foo2\nfoo1 bar1",
		    Files.toString(new File(OUT_TEXT + "/" + "part-r-00000"), Charset.forName("UTF-8")).trim());

		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
		HadoopUtils.deleteIfExists(fS, outPathText);
	}

	@Test
	public void testSplits() throws IOException, InterruptedException, IllegalArgumentException, SecurityException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		testSplits(Long.MAX_VALUE, 20);
		testSplits(1, 20);
		testSplits(20, 40);
	}

	public void testSplits(long maxSplitSize, int generatedRows) throws IOException, InterruptedException, IllegalArgumentException, SecurityException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		logger.info("Testing maxSplitSize: " + maxSplitSize + " and generatedRows:" + generatedRows);
		FileSystem fS = FileSystem.get(getConf());
		Random r = new Random(1);
		Schema schema = new Schema("schema", Fields.parse("i:int,s:string"));
		ITuple tuple = new Tuple(schema);

		Path outPath = new Path(OUT);
		TupleFile.Writer writer = new TupleFile.Writer(FileSystem.get(getConf()), getConf(), outPath, schema);
		for(int i = 0; i < generatedRows; i++) {
			tuple.set("i", r.nextInt());
			tuple.set("s", r.nextLong() + "");
			writer.append(tuple);
		}
		writer.close();

		TupleInputFormat format = ReflectionUtils.newInstance(TupleInputFormat.class, getConf());
		Job job = new Job(getConf());
		FileInputFormat.setInputPaths(job, outPath);
		logger.info("Using max input split size: " + maxSplitSize);
		FileInputFormat.setMaxInputSplitSize(job, maxSplitSize);
		job.setInputFormatClass(FileInputFormat.class);

		// Read all the splits and count. The number of read rows must
		// be the same than the written ones.
		int count = 0;
		for(InputSplit split : format.getSplits(job)) {
			TaskAttemptID attemptId = new TaskAttemptID(new TaskID(), 1);
			TaskAttemptContext attemptContext = TaskAttemptContextFactory.get(getConf(), attemptId);
			logger.info("Sampling split: " + split);
			RecordReader<ITuple, NullWritable> reader = format.createRecordReader(split, attemptContext);
			reader.initialize(split, attemptContext);
			while(reader.nextKeyValue()) {
				tuple = reader.getCurrentKey();
				count++;
			}
			reader.close();
		}

		assertEquals(generatedRows, count);

		HadoopUtils.deleteIfExists(fS, outPath);
	}

}
