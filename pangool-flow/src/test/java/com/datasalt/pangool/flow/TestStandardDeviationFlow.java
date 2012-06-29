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
package com.datasalt.pangool.flow;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.flow.LinearFlow.EXECUTION_MODE;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat.TupleInputReader;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestStandardDeviationFlow extends AbstractHadoopTestLibrary {

	public final static String OUT = "out-" + TestStandardDeviationFlow.class.getName();

	@Test
	public void test() throws Exception {
		String studentsFile = "src/test/resources/test-user-info.txt";
		String scoresFile = "src/test/resources/test-scores-input.txt";

		StandardDeviationFlow flow = new StandardDeviationFlow(studentsFile, scoresFile,OUT,15);
		Configuration conf = new Configuration();
		flow.execute(EXECUTION_MODE.OVERWRITE, conf, "job3.output");
		

		TupleInputReader reader = new TupleInputReader(conf);
		reader.initialize(new Path(OUT + "/part-r-00000"), conf);
		
		reader.nextKeyValueNoSync();
		ITuple tuple = reader.getCurrentKey();
		Assert.assertEquals("ES", tuple.get("country").toString());
		Assert.assertEquals(5.65, tuple.get("average"));
		Assert.assertEquals(5.522500000000001, tuple.get("variance"));
		Assert.assertEquals(2.35, tuple.get("stdev"));
		
		reader.nextKeyValueNoSync();
		tuple = reader.getCurrentKey();
		Assert.assertEquals("UK", tuple.get("country").toString());
		Assert.assertEquals(7.6, tuple.get("average"));
		Assert.assertEquals(0.0, tuple.get("variance"));
		Assert.assertEquals(0.0, tuple.get("stdev"));
		
		reader.nextKeyValueNoSync();
		tuple = reader.getCurrentKey();
		Assert.assertEquals("US", tuple.get("country").toString());
		Assert.assertEquals(6.15, tuple.get("average"));
		Assert.assertEquals(2.7224999999999997, tuple.get("variance"));
		Assert.assertEquals(1.65, tuple.get("stdev"));
		
		reader.close();
		
		trash(OUT, "job1.output","job2.output","job3.output");
	}
}
