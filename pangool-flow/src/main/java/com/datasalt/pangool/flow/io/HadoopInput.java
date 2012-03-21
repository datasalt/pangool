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

import org.apache.hadoop.mapreduce.InputFormat;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMapper;

/**
 * Input spec for any Hadoop-based input. Contains the intermediate schemas that this input will promote.
 */
@SuppressWarnings("rawtypes")
public class HadoopInput implements RichInput {

  final private InputFormat format;
  final private TupleMapper processor;
  final private Schema[] intermediateSchemas;
	
	public HadoopInput(InputFormat format, TupleMapper processor, Schema... intermediateSchemas) {
		this.format = format;
	  this.processor = processor;
	  this.intermediateSchemas = intermediateSchemas;
	}

	public InputFormat getFormat() {
  	return format;
  }

	public TupleMapper getProcessor() {
  	return processor;
  }

	public Schema[] getIntermediateSchemas() {
  	return intermediateSchemas;
  }
}
