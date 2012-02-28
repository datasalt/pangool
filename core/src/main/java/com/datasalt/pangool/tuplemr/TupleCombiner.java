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
package com.datasalt.pangool.tuplemr;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.TupleReducer.StaticTupleMRContext;

@SuppressWarnings("serial")
public class TupleCombiner implements Serializable {

	public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException, TupleMRException {

	}

	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException,
  InterruptedException, TupleMRException {

	}
	
	public void cleanup(TupleMRContext context, Collector collector) throws IOException, InterruptedException,
	    TupleMRException {

	}

	/* ------------ INNER CLASSES ------------ */	
	
	/**
	 * A class for collecting data inside a {@link TupleCombiner}.
	 */
	public static final class Collector {
		
    private ReduceContext<DatumWrapper<ITuple>, NullWritable, DatumWrapper<ITuple>, NullWritable> context;

    private ThreadLocal<DatumWrapper<ITuple>> cachedDatum = new ThreadLocal<DatumWrapper<ITuple>>(){
			@Override
			public DatumWrapper<ITuple> get(){
				return new DatumWrapper<ITuple>();
			}
		};
    
    private NullWritable nullWritable = NullWritable.get();
    
		public Collector(TupleMRConfig pangoolConfig, ReduceContext<DatumWrapper<ITuple>, NullWritable, DatumWrapper<ITuple>, NullWritable> context){
			this.context = context;
		}
		
    public void write(ITuple tuple) throws IOException,InterruptedException {
    	DatumWrapper<ITuple> outputDatum = cachedDatum.get();
    	outputDatum.datum(tuple);
			context.write(outputDatum, nullWritable);
		}
	}
  
  public class TupleMRContext extends StaticTupleMRContext<DatumWrapper<ITuple>, NullWritable> {
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the CoGrouperContext meanwhile keeping generics. 
		 */
		public TupleMRContext(ReduceContext<DatumWrapper<ITuple>, NullWritable, DatumWrapper<ITuple>, NullWritable> hadoopContext,
        TupleMRConfig pangoolConfig) {
      super(hadoopContext, pangoolConfig);
    }    	
  }
}
