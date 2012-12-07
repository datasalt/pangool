package com.datasalt.pangool.flow;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

// TODO Extract common interface between OperativeStep and MRStep
public abstract class OperativeStep {

	public static int N_OP_STEPS = 0;
	String name;
	
	public OperativeStep() {
		N_OP_STEPS++;
		name = "operativeStep" + N_OP_STEPS;
	}
	
	transient LinkedHashMap<String, MRInput> bindings = new LinkedHashMap<String, MRInput>();
	
	public void addDependency(MRInput mrInput) {
		String inputName = "input" + bindings.keySet().size();
		bindings.put(inputName, mrInput);
	}
	
	public abstract int run(Path outputPath, Map<String, Path> parsedInputs)  throws Exception;
	
	@SuppressWarnings("serial")
	public Step getStep() {

		return new Step(name, new Inputs(bindings.keySet().toArray(new String[0]))) {
			@Override
			public int run(Path outputPath, Map<String, Path> parsedInputs,
			    Map<String, Object> parsedParameters) throws Exception {
				
				return OperativeStep.this.run(outputPath, parsedInputs);
			}
		};
	}
}
