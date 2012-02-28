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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.tuplemr.mapred.lib.output.PangoolMultipleOutputs;

public class TestPangoolMultipleOutputs {

	@Test
	public void testSpecificContext() throws IOException {
		// Test that we can add specific key, value configurations for each output
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		PangoolMultipleOutputs.addNamedOutputContext(job, "foo", "my.context.property", "myValue");

		PangoolMultipleOutputs.setSpecificNamedOutputContext(job.getConfiguration(), job, "foo");
		Assert.assertEquals("myValue", job.getConfiguration().get("my.context.property"));
	}
}
