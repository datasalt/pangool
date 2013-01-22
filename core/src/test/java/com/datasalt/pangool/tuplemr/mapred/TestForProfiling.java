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

package com.datasalt.pangool.tuplemr.mapred;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

/**
 * Unit tests useful to profile Pangool. Desactivated by default.
 */
public class TestForProfiling extends BaseTest implements Serializable {

	int NUM_ROWS_TO_GENERATE = 30000;

	public void runningIdentityJob(boolean withNulls) throws IOException, ClassNotFoundException,
	    InterruptedException, TupleMRException {

		Configuration conf = getConf();
		String input = TestTupleMRJob.class + "-input";
		String output = TestTupleMRJob.class + "-output";

		Schema schema = SCHEMA;
		if(withNulls) {
			decorateWithNullables(schema);
		}

		ITuple tuple = new Tuple(schema);
		for(int i = 0; i < NUM_ROWS_TO_GENERATE; i++) {
			withTupleInput(input, fillTuple(true, tuple));
		}

		TupleMRBuilder builder = new TupleMRBuilder(getConf(), "test");
		builder.addTupleInput(new Path(input), new IdentityTupleMapper());
		builder.setTupleReducer(new IdentityTupleReducer());
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields(schema.getField(0).getName());
		builder.setTupleOutput(new Path(output), schema);

		Job job = builder.createJob();
		try {
			job.setNumReduceTasks(1);
			assertRun(job);
		} finally {
			builder.cleanUpInstanceFiles();
		}
		trash(input);
		trash(output);
	}

	@Test
	@Ignore
	public void runninIdentityJobWithNulls() throws IOException, TupleMRException, ClassNotFoundException,
	    InterruptedException {
		runningIdentityJob(true);
	}

	@Test
	@Ignore
	public void runninIdentityJobNoNulls() throws IOException, TupleMRException, ClassNotFoundException,
	    InterruptedException {
		runningIdentityJob(false);
	}
}
