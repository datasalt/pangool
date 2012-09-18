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

import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.flow.ops.Op;
import com.datasalt.pangool.flow.ops.ReturnCallback;
import com.datasalt.pangool.flow.ops.TupleReduceOp;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMRException;

@SuppressWarnings("serial")
/**
 * Reducer to be used to execute one {@link Op} or {@link ChainOp}. Removes the need of implementing a reducer.
 * It can be used with {@link TupleOutput}s. 
 */
public class TupleOpReducer extends SingleSchemaReducer {

	Op<Iterable<ITuple>, ITuple> op;
	Collector collector;
	
	public TupleOpReducer(TupleReduceOp op) {
	  super(op.getSchema());
	  this.op = (Op<Iterable<ITuple>, ITuple>)op;
  }
	
	public TupleOpReducer(Op<Iterable<ITuple>, ITuple> op, Schema outSchema) {
		super(outSchema);
		this.op = op;
	}
	
	public void setup(TupleMRContext tupleMRContext, Collector collector) throws IOException ,InterruptedException ,TupleMRException {
		this.collector = collector;
	};

	ReturnCallback<ITuple> callback = new ReturnCallback<ITuple>() {

		@Override
    public void onReturn(ITuple element) {
	    if(element != null) {
	    	try {
	        collector.write(element, NullWritable.get());
        } catch(IOException e) {
	        throw new RuntimeException(e);
        } catch(InterruptedException e) {
	        throw new RuntimeException(e);
        }
	    }
    }
	};
	
	@Override
	public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
	    throws IOException, InterruptedException, TupleMRException {
		
		op.process(tuples, callback);
	}
}
