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
package com.datasalt.pangool.solr;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

/**
 * Example usage of {@TupleSolrOutputFormat} that is also used for unit testing.
 * <p>
 * This example creates three index: one in the main output which contains (user_id, message) pairs of english messages and
 * two other auxiliar indexes with french & spanish messages.
 */
@SuppressWarnings("serial")
public class TupleSolrOutputFormatExample implements Serializable {

	public int run(String input, String output, Configuration conf) throws Exception {
		// Define the intermediate schema: It must match SOLR's schema.xml!
		final Schema schema = new Schema("iSchema", Fields.parse("user_id:string, message:string"));

		TupleMRBuilder job = new TupleMRBuilder(conf);
		job.addIntermediateSchema(schema);
		job.setGroupByFields("user_id");
		// Define the input and its associated mapper.
		// We'll just have a Mapper, reducer will be Identity
		job.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new TupleMapper<LongWritable, Text>() {

			Tuple tuple = new Tuple(schema);

			@Override
			public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
			    InterruptedException {
				String[] fields = value.toString().split("\t");
				String language = fields[1];
				tuple.set("user_id", fields[0]);
				tuple.set("message", fields[2]);
				if(language.equals("en")) {
					// English -> write to main output
					collector.write(tuple);
				} else if(language.equals("fr")) {
					// French -> write to french index
					collector.getNamedOutput("FR").write(tuple, NullWritable.get());
				} else if(language.equals("es")) {
					// Spanish -> write to spanish index
					collector.getNamedOutput("ES").write(tuple, NullWritable.get());
				}
			}
		});
		// Add multi-output: French index
		job.addNamedOutput("FR",  new TupleSolrOutputFormat(new File("src/test/resources/solr-fr"), conf), ITuple.class, NullWritable.class);
		// Add multi-output: Spanish index
		job.addNamedOutput("ES",  new TupleSolrOutputFormat(new File("src/test/resources/solr-es"), conf), ITuple.class, NullWritable.class);
		job.setTupleReducer(new IdentityTupleReducer());
		// Add multi-output: English index
		job.setOutput(new Path(output), new TupleSolrOutputFormat(new File("src/test/resources/solr-en"), conf), ITuple.class, NullWritable.class);
		Job hadoopJob = job.createJob();
		hadoopJob.waitForCompletion(true);
		if (!hadoopJob.isSuccessful()){
			throw new PangoolRuntimeException("Job was not sucessfull");
		}
		return 0;
	}
}