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

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * This is a very dummy example flow that does nothing but will assure that things are executed in the appropriated order.
 * It shows the usage of {@link Step}.
 */
@SuppressWarnings("serial")
public class ExampleFlow extends BaseFlow {

	ArrayList<String> executedJobs = new ArrayList<String>();

	// Job1 has one input file and one integer parameter
  public class Job1 extends Step {

		public Job1() {
			super("job1", new Inputs("inputFile"), new Params(
			    new Param("param1", Integer.class, "An integer between 0 and 5")));
		}

		@Override
		public int run(Path outputPath, Map<String, Path> parsedInputs, Map<String, Object> parsedParameters)
		    throws Exception {
			executedJobs.add("job1");
			return 1;
		}
	}

	// Job2 has one input file and one named output
	public class Job2 extends Step {

		public Job2() {
			super("job2", new Inputs("inputFile"), Params.NONE, new NamedOutputs("secondaryOutput"),
			    "Job2 is different because it has a named output.");
		}

		@Override
		public int run(Path outputPath, Map<String, Path> parsedInputs, Map<String, Object> parsedParameters)
		    throws Exception {
			executedJobs.add("job2");
			return 1;
		}
	}

	// Job3 has 3 input files
	public class Job3 extends Step {

		public Job3() {
			super("job3", new Inputs("inputFile1", "inputFile2", "inputFile3"));
		}

		@Override
		public int run(Path outputPath, Map<String, Path> parsedInputs, Map<String, Object> parsedParameters)
		    throws Exception {
			executedJobs.add("job3");
			return 1;
		}
	}

	public ExampleFlow() {
		// Add the involved Jobs - no particular order is needed here
		add(new Job2());
		add(new Job3());
		add(new Job1());

		// Add the input files to the Flow - we need something to start with!
		add("my-input.txt");

		// We can bind constant objects to declared resources
		bind("job1.param1", 10); 
		
		// Define the relationships within resources - this is the most important part!
		// Order is not relevant as well
		bind("job1.inputFile", "my-input.txt"); // job1 consumes input file
		bind("job2.inputFile", "my-input.txt"); // job2 consumes input file
		
		bind("job3.inputFile1", "job1.output"); // make job1's output be the input1 of job3
		bind("job3.inputFile2", "job2.output"); // make job2's output be the input2 of job3
		bind("job3.inputFile3", "job2.output.secondaryOutput"); // make job2's named output "secondaryOutput" be the input3 of job3
		
		// nothing more to define
	}
}
