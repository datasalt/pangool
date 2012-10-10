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

import com.datasalt.pangool.flow.mapred.TupleOpMapper;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.TupleMapper;

/**
 * Input spec for tuple inputs. Contains the intermediate schemas that this input will promote.
 */
@SuppressWarnings("rawtypes")
public class TupleInput implements RichInput {

	final private Schema[] intermediateSchemas;
	final private TupleMapper processor;

	public TupleInput(Schema... intermediateSchemas) {
		this(new IdentityTupleMapper(), intermediateSchemas);
	}
	
	public TupleInput(TupleOpMapper mapper) {
		this.intermediateSchemas = new Schema[] { mapper.getOp().getSchema() };
		this.processor = mapper;
	}
	
	public TupleInput(TupleMapper processor, Schema... intermediateSchemas) {
	  this.intermediateSchemas = intermediateSchemas;
	  this.processor = processor;
	}

	public Schema[] getIntermediateSchemas() {
  	return intermediateSchemas;
  }

	public TupleMapper getProcessor() {
  	return processor;
  }
}
