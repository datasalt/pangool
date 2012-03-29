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

@SuppressWarnings("serial")
public class NamedOutputs extends ArrayList<String> {

	public final static String OUTPUT = "output";
	
	public NamedOutputs(String... outputs) {
		super(outputs.length);
		for(String output: outputs) {
			if(output.equals(OUTPUT)) {
				throw new IllegalArgumentException("can't use reserved output name: [" + output + "] - reserved for main output.");
			}
			add(output);
		}
	}
	
	public static NamedOutputs NONE = new NamedOutputs();
}
