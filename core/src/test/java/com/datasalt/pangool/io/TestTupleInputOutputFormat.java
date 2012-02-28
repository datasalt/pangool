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
package com.datasalt.pangool.io;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.cogroup.TupleMRBuilder;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.processors.IdentityTupleReducer;
import com.datasalt.pangool.cogroup.processors.IdentityTupleMapper;
import com.datasalt.pangool.cogroup.processors.TupleMapper;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.test.AbstractHadoopTestLibrary;
import com.datasalt.pangool.utils.CommonUtils;
import com.datasalt.pangool.utils.HadoopUtils;
import com.google.common.io.Files;

public class TestTupleInputOutputFormat extends AbstractHadoopTestLibrary {

	public static String OUT = TestTupleInputOutputFormat.class.getName() + "-out";
	public static String OUT_TEXT = TestTupleInputOutputFormat.class.getName() + "-out-text";
	public static String IN = TestTupleInputOutputFormat.class.getName() + "-in";

	public static class MyInputProcessor extends TupleMapper<LongWritable, Text> {

    private static final long serialVersionUID = 1L;
		private Tuple tuple;

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			if (tuple == null){
				tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema(0));
			}
			tuple.set(0, "title");
			tuple.set(1, value);
			collector.write(tuple);
		}
	}

	public static class MyGroupHandler extends TupleReducer<Text, Text> {

    private static final long serialVersionUID = 1L;

    @Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
		    Collector collector) throws IOException, InterruptedException, TupleMRException {
			for(ITuple tuple : tuples) {
				collector.write((Text)tuple.get(0),(Text)tuple.get(1));
			}
		}
	}

	@Test
	public void test() throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException {

		CommonUtils.writeTXT("foo1 bar1\nbar2 foo2", new File(IN));
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		Path outPath = new Path(OUT);
		Path inPath = new Path(IN);
		Path outPathText = new Path(OUT_TEXT);
		HadoopUtils.deleteIfExists(fS, outPath);
		HadoopUtils.deleteIfExists(fS, outPathText);

		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("title",Utf8.class));
		fields.add(new Field("content",Utf8.class));
		Schema schema = new Schema("schema",fields);
		
		TupleMRBuilder coGrouper = new TupleMRBuilder(conf);
		coGrouper.addIntermediateSchema(schema);
		coGrouper.setGroupByFields("title");
		coGrouper.setOrderBy(new SortBy().add("title",Order.ASC).add("content",Order.ASC));

		coGrouper.setTupleReducer(new IdentityTupleReducer());
		coGrouper.setTupleOutput(outPath, schema); // setTupleOutput method
		coGrouper.addInput(inPath, new HadoopInputFormat(TextInputFormat.class), new MyInputProcessor());

		coGrouper.createJob().waitForCompletion(true);

		// Use output as input of new CoGrouper

		coGrouper = new TupleMRBuilder(conf);
		coGrouper.addIntermediateSchema(schema);
		coGrouper.setGroupByFields("title");
		coGrouper.setOrderBy(new SortBy().add("title",Order.ASC).add("content",Order.ASC));
		coGrouper.setTupleReducer(new MyGroupHandler());
		coGrouper.setOutput(outPathText, new HadoopOutputFormat(TextOutputFormat.class), Text.class, Text.class);
		coGrouper.addTupleInput(outPath, new IdentityTupleMapper()); // addTupleInput method
		Job job = coGrouper.createJob();
		assertRun(job);

		Assert.assertEquals("title\tbar2 foo2\ntitle\tfoo1 bar1",
		    Files.toString(new File(OUT_TEXT + "/" + "part-r-00000"), Charset.forName("UTF-8")).trim());

		HadoopUtils.deleteIfExists(fS, inPath);
		HadoopUtils.deleteIfExists(fS, outPath);
		HadoopUtils.deleteIfExists(fS, outPathText);
	}
}
