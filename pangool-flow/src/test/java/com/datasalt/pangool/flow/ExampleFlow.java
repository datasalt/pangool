package com.datasalt.pangool.flow;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.Path;

public class ExampleFlow extends LinearFlow {

	ArrayList<String> executedJobs = new ArrayList<String>();

	// Job1 has one input file and one integer parameter
	public class Job1 extends PangoolJob {

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
	public class Job2 extends PangoolJob {

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
	public class Job3 extends PangoolJob {

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
		bind("job3.inputFile3", "job2.secondaryOutput"); // make job2's named output "secondaryOutput" be the input3 of job3
		
		// nothing more to define
	}
}
