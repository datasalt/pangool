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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.flow.LinearFlow.EXECUTION_MODE;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestTopCountryBySimilarsFlow extends AbstractHadoopTestLibrary {

	public final static String OUT = "out-" + TestTopCountryBySimilarsFlow.class.getName();

	@Test
	public void test() throws Exception {
		String similarityTestFile = "src/test/resources/test-similarity-input.txt";
		String countryInfoTestFile = "src/test/resources/test-country-info-input.txt";
		int topSize = 2;

		TopCountryBySimilarsFlow flow = new TopCountryBySimilarsFlow(similarityTestFile, countryInfoTestFile, topSize, OUT);
		Configuration conf = new Configuration();
		flow.execute(EXECUTION_MODE.OVERWRITE, conf, "topCountry.output");

		final Map<String, String> resultMap = new HashMap<String, String>();
		Utils.readTuples(new Path(OUT + "/part-r-00000"), conf, new Utils.TupleVisitor() {

			@Override
			public void onTuple(ITuple tuple) {
				resultMap.put(tuple.get("first").toString(), tuple.get("country").toString());
			}
		});

		assertEquals("ES", resultMap.get("Jon"));
		assertEquals("UK", resultMap.get("Pere"));

		trash(OUT, "topSimilarities.output", "attachCountry.output");
	}
}
