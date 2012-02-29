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
package com.datasalt.pangool.examples.urlresolution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

/**
 * This example shows how to perform reduce-side joins using Pangool. We have one file with URL Registers: ["url", "timestamp", "ip"] and another file with
 * canonical URL mapping: ["url", "canonicalUrl"]. We want to obtain the URL Registers file with the url substituted with the
 * canonical one according to the mapping file: ["canonicalUrl", "timestamp", "ip"].
 */
public class UrlResolution extends BaseExampleJob {

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

	public UrlResolution() {
		super("UrlResolution: [input_url_mapping] [input_url_regs] [output]");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			failArguments("Wrong number of arguments");
			return -1;
		}
		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];
		
		deleteOutput(output);
		
		List<Field> urlRegisterFields = new ArrayList<Field>();
		urlRegisterFields.add(Field.create("url",Type.STRING));
		urlRegisterFields.add(Field.create("timestamp",Type.LONG));
		urlRegisterFields.add(Field.create("ip",Type.STRING));

		List<Field> urlMapFields = new ArrayList<Field>();
		urlMapFields.add(Field.create("url",Type.STRING));
		urlMapFields.add(Field.create("canonicalUrl",Type.STRING));

		TupleMRBuilder grouper = new TupleMRBuilder(conf,"Pangool Url Resolution");
		grouper.addIntermediateSchema(new Schema("urlMap", urlMapFields));
		grouper.addIntermediateSchema(new Schema("urlRegister", urlRegisterFields));
		grouper.setGroupByFields("url");
		grouper.setTupleReducer(new Handler());
		grouper.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		grouper.addInput(new Path(input1), new HadoopInputFormat(TextInputFormat.class), new UrlMapProcessor());
		grouper.addInput(new Path(input2), new HadoopInputFormat(TextInputFormat.class), new UrlProcessor());
		grouper.createJob().waitForCompletion(true);

		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new UrlResolution(), args);
	}
}
