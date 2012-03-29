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
package com.datasalt.pangool.flow.io;

import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * Output spec for any Hadoop-based output.
 */
@SuppressWarnings("rawtypes")
public class HadoopOutput implements RichOutput {
	
	final private Class key;
	final private Class value;
	final private OutputFormat outputFormat;
	
	public HadoopOutput(OutputFormat outputFormat, Class key, Class value) {
		this.key = key;
		this.value = value;
		this.outputFormat = outputFormat;
	}

	public Class getKey() {
  	return key;
  }

	public Class getValue() {
  	return value;
  }

	public OutputFormat getOutputFormat() {
  	return outputFormat;
  }
}
