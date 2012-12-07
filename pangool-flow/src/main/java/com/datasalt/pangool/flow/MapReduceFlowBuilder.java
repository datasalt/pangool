package com.datasalt.pangool.flow;

import java.util.Map;

@SuppressWarnings("serial")
public class MapReduceFlowBuilder extends LinearFlow {

	public void addInput(ResourceMRInput input) {
		super.add(input.getId());
	}
	
	public void addStep(MRStep mrStep) {
		for(Map.Entry<String, MRInput> binding: mrStep.bindings.entrySet()) {
			bind(mrStep.name + "." + binding.getKey(), binding.getValue().getId());
		}
		
		int param = 0;
		for(OperativeStep st: mrStep.extraDependencies) {
			bind(mrStep.name + ".extradep" + param, st.name + ".output");
			param++;
		}
		
		add(mrStep.getStep());
	}
	
	public void addStep(OperativeStep operativeStep) {
		for(Map.Entry<String, MRInput> binding: operativeStep.bindings.entrySet()) {
			bind(operativeStep.name + "." + binding.getKey(), binding.getValue().getId());
		}
		
		add(operativeStep.getStep());
	}
	
	public void bindOutput(MRInput mrInput, String outputName) {
		bind(mrInput.getId(), outputName);
	}
	
	public LinearFlow getFlow() {
		return this;
	}
}
