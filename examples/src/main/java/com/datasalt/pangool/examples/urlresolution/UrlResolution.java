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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Aliases;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;
import static com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat.*;

/**
 * This example shows how to perform reduce-side joins using Pangool. We have one file with URL Registers: ["url",
 * "timestamp", "ip"] and another file with canonical URL mapping: ["url", "canonicalUrl"]. We want to obtain the URL
 * Registers file with the url substituted with the canonical one according to the mapping file: ["canonicalUrl",
 * "timestamp", "ip"].
 */
public class UrlResolution extends BaseExampleJob {

	static Schema getURLRegisterSchema() {
		return new Schema("urlRegister", Fields.parse("url:string, timestamp:long, ip:string"));
	}

	static Schema getURLMapSchema() {
		return new Schema("urlMap", Fields.parse("nonCanonicalUrl:string, canonicalUrl:string"));
	}

	@SuppressWarnings("serial")
	public static class Handler extends TupleReducer<ITuple, NullWritable> {

		private Tuple result;

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {
			if(result == null) {
				result = new Tuple(getURLRegisterSchema());
			}
			String cannonicalUrl = null;
			for(ITuple tuple : tuples) {
				if("urlMap".equals(tuple.getSchema().getName())) {
					cannonicalUrl = tuple.getString("canonicalUrl");
				} else {
					result.set("url", cannonicalUrl);
					result.set("timestamp", tuple.get("timestamp"));
					result.set("ip", tuple.get("ip"));
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

		delete(output);

		TupleMRBuilder mr = new TupleMRBuilder(conf, "Pangool Url Resolution");
		mr.addIntermediateSchema(getURLMapSchema());
		mr.addIntermediateSchema(getURLRegisterSchema());
		mr.addInput(new Path(input1), new TupleTextInputFormat(getURLMapSchema(), false, false, '\t',
		    NO_QUOTE_CHARACTER, NO_ESCAPE_CHARACTER, null, null), new IdentityTupleMapper());
		mr.addInput(new Path(input2), new TupleTextInputFormat(getURLRegisterSchema(), false, false, '\t',
		    NO_QUOTE_CHARACTER, NO_ESCAPE_CHARACTER, null, null), new IdentityTupleMapper());
		mr.setFieldAliases("urlMap", new Aliases().add("url", "nonCanonicalUrl"));
		mr.setGroupByFields("url");
		mr.setOrderBy(new OrderBy().add("url", Order.ASC).addSchemaOrder(Order.ASC));
		mr.setSpecificOrderBy("urlRegister", new OrderBy().add("timestamp", Order.ASC));
		mr.setTupleReducer(new Handler());
		mr.setOutput(new Path(output), new TupleTextOutputFormat(getURLRegisterSchema(), false, '\t',
		    NO_QUOTE_CHARACTER, NO_ESCAPE_CHARACTER), ITuple.class, NullWritable.class);

		try {
			mr.createJob().waitForCompletion(true);
		} finally {
			mr.cleanUpInstanceFiles();
		}

		return 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new UrlResolution(), args);
	}
}
