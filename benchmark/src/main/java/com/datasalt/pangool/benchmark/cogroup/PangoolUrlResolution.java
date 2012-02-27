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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.cogroup.TupleMRBuilder;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.processors.TupleMapper;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.io.HadoopInputFormat;
import com.datasalt.pangool.io.HadoopOutputFormat;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.utils.HadoopUtils;

/**
 * Code for solving the URL Resolution CoGroup Problem in Pangool.
 * <p>
 * The URL Resolution CoGroup Problem is: We have one file with URL Registers: {url timestamp ip} and another file with
 * canonical URL mapping: {url canonicalUrl}. We want to obtain the URL Registers file with the url substituted with the
 * canonical one according to the mapping file: {canonicalUrl timestamp ip}.
 */
public class PangoolUrlResolution {

	@SuppressWarnings("serial")
	public static class UrlProcessor extends TupleMapper<LongWritable, Text> {
		private Tuple tuple;

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {

			if(tuple == null) {
				tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("urlRegister"));
			}
			String[] fields = value.toString().split("\t");
			tuple.set("url", fields[0]);
			tuple.set("timestamp", Long.parseLong(fields[1]));
			tuple.set("ip", fields[2]);
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class UrlMapProcessor extends TupleMapper<LongWritable, Text> {

		private Tuple tuple;

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			if(tuple == null) {
				tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("urlMap"));
			}

			String[] fields = value.toString().split("\t");
			tuple.set("url", fields[0]);
			tuple.set("canonicalUrl", fields[1]);
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Handler extends TupleReducer<Text, NullWritable> {

		private Text result;

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			if (result == null) {
				result = new Text();
			}
			String cannonicalUrl = null;
			for(ITuple tuple : tuples) {
				if("urlMap".equals(tuple.getSchema().getName())) {
					cannonicalUrl = tuple.get("canonicalUrl").toString();
				} else {
					result.set(cannonicalUrl + "\t" + tuple.get("timestamp") + "\t" + tuple.get("ip"));
					collector.write(result, NullWritable.get());
				}
			}
		}
	}

	public Job getJob(Configuration conf, String input1, String input2, String output) throws TupleMRException,
	    IOException {
		List<Field> urlRegisterFields = new ArrayList<Field>();
		urlRegisterFields.add(new Field("url", Utf8.class));
		urlRegisterFields.add(new Field("timestamp", Long.class));
		urlRegisterFields.add(new Field("ip", Utf8.class));

		List<Field> urlMapFields = new ArrayList<Field>();
		urlMapFields.add(new Field("url", Utf8.class));
		urlMapFields.add(new Field("canonicalUrl", Utf8.class));

		TupleMRBuilder grouper = new TupleMRBuilder(conf,"Pangool Url Resolution");
		grouper.addIntermediateSchema(new Schema("urlMap", urlMapFields));
		grouper.addIntermediateSchema(new Schema("urlRegister", urlRegisterFields));
		

		grouper.setGroupByFields("url");
		grouper.setOrderBy(new SortBy().add("url", Order.ASC).addSourceOrder(Order.ASC));
		
		//grouper.setSecondaryOrderBy("urlRegister", new SortBy().add("timestamp", Order.DESC));

		grouper.setTupleReducer(new Handler());
		grouper.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		grouper.addInput(new Path(input1), new HadoopInputFormat(TextInputFormat.class), new UrlMapProcessor());
		grouper.addInput(new Path(input2), new HadoopInputFormat(TextInputFormat.class), new UrlProcessor());
		return grouper.createJob();
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException,
	    TupleMRException {

		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];
		HadoopUtils.deleteIfExists(fS, new Path(output));
		new PangoolUrlResolution().getJob(conf, input1, input2, output).waitForCompletion(true);
	}
}
