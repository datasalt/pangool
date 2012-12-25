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
