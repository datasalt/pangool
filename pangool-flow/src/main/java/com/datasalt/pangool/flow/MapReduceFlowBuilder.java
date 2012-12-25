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

import java.util.Map;

/**
 * A builder for generating (linear) flows that can be later executed. This is the entry point to pangool-flow.
 * <p>
 * Use {@link #addInput(ResourceMRInput)} to add an input to the flow. Inputs can either be {@link ResourceMRInput} if they are
 * an external resource (for example a file) or {@link MRInput} if they are one of the outputs of a {@link MRStep}. Inputs of a flow
 * can only be {@link ResourceMRInput}. 
 * <p>
 * Use {@link #addStep(MRStep)} to add a step to the flow. The order in which steps are added doesn't matter.
 * <p>
 * Use {@link #bindOutput(MRInput, String)} to define the outputs of your flow.
 */
@SuppressWarnings("serial")
public class MapReduceFlowBuilder extends BaseFlow {

	/**
	 * Adds an input to the flow. 
	 */
	public void addInput(ResourceMRInput input) {
		super.add(input.getId());
	}
	
	/**
	 * Adds a step. The dependency tree will be calculated afterwards.
	 */
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
	
	/**
	 * Adds a special step {@link OperativeStep} which allows us to execute any operation (not a MapReduce Job).
	 */
	public void addStep(OperativeStep operativeStep) {
		for(Map.Entry<String, MRInput> binding: operativeStep.bindings.entrySet()) {
			bind(operativeStep.name + "." + binding.getKey(), binding.getValue().getId());
		}
		
		add(operativeStep.getStep());
	}
	
	/**
	 * Associates the output of a Step to a folder name - which will be the output of the flow or one of the outputs of the flow.
	 */
	public void bindOutput(MRInput mrInput, String outputName) {
		bind(mrInput.getId(), outputName);
	}
	
	/**
	 * Return the flow ready to be executed.
	 */
	public BaseFlow getFlow() {
		return this;
	}
}
