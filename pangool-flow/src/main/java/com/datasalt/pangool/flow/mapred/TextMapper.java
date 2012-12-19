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
package com.datasalt.pangool.flow.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.datasalt.pangool.flow.io.SequenceInput;
import com.datasalt.pangool.flow.io.TextInput;
import com.datasalt.pangool.flow.ops.ChainOp;
import com.datasalt.pangool.flow.ops.ReturnCallback;
import com.datasalt.pangool.flow.ops.TupleOp;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMapper;

/**
 * Mapper to be used to execute one {@link TupleOp} or {@link ChainOp}. Removes the need of implementing a Mapper.
 * Reads a Key, Value file with Text as second type and passes this Text.toString() to ops.
 * It can be used with {@link TextInput}s or {@link SequenceInput}s with textual content as value.
 */
@SuppressWarnings("serial")
public class TextMapper extends TupleMapper<Object, Text> {

	TupleOp<String> op;
	Collector collector;
	Schema intermediateSchema;
	
	public TextMapper(TupleOp<String> op) {
		this.op = op;
		this.intermediateSchema = op.getSchema();
		if(intermediateSchema == null) {
			throw new IllegalArgumentException("TupleOp must return a schema for the Job to be properly configured with addIntermediateSchema()!");
		}
	}

	public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
		this.collector = collector;
	}
	
	ReturnCallback<ITuple> callback = new ReturnCallback<ITuple>() {

		@Override
    public void onReturn(ITuple element) {
	    if(element != null) {
	    	try {
	        collector.write(element);
        } catch(IOException e) {
	        throw new RuntimeException(e);
        } catch(InterruptedException e) {
	        throw new RuntimeException(e);
        }
	    }
    }
	};
	
	@Override
	public void map(Object keyToIgnore, Text value, TupleMRContext context, Collector collector) throws IOException,
	    InterruptedException {
		
		op.process(value.toString(), callback);
	}

	public Schema getIntermediateSchema() {
  	return intermediateSchema;
  }
}
