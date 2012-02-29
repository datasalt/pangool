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
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.mapred.RollupReducer;
import com.datasalt.pangool.tuplemr.mapred.SimpleReducer;

/**
 * 
 * This is the common interface that any {@link TupleMRBuilder} job needs to implement. This handler is called in the reducer
 * step by {@link SimpleReducer} or {@link RollupReducer} depending if Roll-up feature is used.
 */
@SuppressWarnings("serial")
public class TupleReducer<OUTPUT_KEY, OUTPUT_VALUE> implements Serializable {
	
	public void setup(TupleMRContext tupleMRContext, Collector collector)
	    throws IOException, InterruptedException, TupleMRException {

	}

	/**
	 * 
	 * This method is called with an iterable that contains all the tuples that have been grouped by the fields defined
	 * in {@link TupleMRConfigBuilder#setGroupByFields(String...)}
	 * 
	 * @param tuples
	 *          Iterable that contains all the tuples from a group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 */
	public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext tupleMRContext, Collector collector) throws IOException, InterruptedException,
	    TupleMRException {

	}
	
	public void cleanup(TupleMRContext tupleMRContext, Collector collector)
	    throws IOException, InterruptedException, TupleMRException {
	}
	
	/* ------------ INNER CLASSES ------------ */
	
	/**
	 * 	A base class for the {@link TupleReducer.Collector}
	 */
	public static class StaticCollector<OUTPUT_KEY, OUTPUT_VALUE, CONTEXT_OUTPUT_KEY, CONTEXT_OUTPUT_VALUE> extends MultipleOutputsCollector {

		protected ReduceContext<DatumWrapper<ITuple>, NullWritable, CONTEXT_OUTPUT_KEY, CONTEXT_OUTPUT_VALUE> context;
		
    public StaticCollector(ReduceContext<DatumWrapper<ITuple>, NullWritable, CONTEXT_OUTPUT_KEY, CONTEXT_OUTPUT_VALUE> context) {
	    super(context);
	    this.context = context;
    }
		
		@SuppressWarnings("unchecked")
    public void write(OUTPUT_KEY key, OUTPUT_VALUE value) throws IOException, InterruptedException { 
			context.write((CONTEXT_OUTPUT_KEY) key, (CONTEXT_OUTPUT_VALUE) value);
		}
	}
	
	/**
	 * Base class for collecting data from a {@link TupleReducer} in the reduce phase.
   * This non static inner class is created to eliminate the need in
	 * of the extended TupleReducer methods to specify the generic types
	 * for the Collector meanwhile keeping generics. 
	 */
	public class Collector extends StaticCollector<OUTPUT_KEY, OUTPUT_VALUE, Object, Object> {
		public Collector(ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object> context) {
	    super(context);
    }
	}
		
	/**
	 * Class for collecting data from a {@link TupleReducer} in the combining phase
	 */
	public class CombinerCollector extends Collector {
		
		private ThreadLocal<DatumWrapper<ITuple>> cachedDatum = new ThreadLocal<DatumWrapper<ITuple>>(){
			@Override
			public DatumWrapper<ITuple> get(){
				return new DatumWrapper<ITuple>();
			}
		};
			
		private NullWritable nullWritable;
		
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the Collector meanwhile keeping generics. 
		 */
		public CombinerCollector(
				ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object> context) {
	    super(context);
			nullWritable = NullWritable.get();
		}
		
		/**
		 * Overrided write for wrapping tuples into DatumWrappers.  
		 */
		@Override
    public void write(OUTPUT_KEY tuple, OUTPUT_VALUE ignored) throws IOException, InterruptedException {
			DatumWrapper<ITuple> outputDatum = cachedDatum.get();
			outputDatum.datum((ITuple) tuple);
			context.write(outputDatum, nullWritable);
		}
	}  
	
  public static class TupleMRContext {
  	
  	private TupleMRConfig pangoolConfig;
  	private ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object> hadoopContext;
  	
  	public TupleMRContext(ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object> hadoopContext, TupleMRConfig pangoolConfig) {
  		this.pangoolConfig = pangoolConfig;
  		this.hadoopContext = hadoopContext;
  	}

  	public TupleMRConfig getTupleMRConfig() {
  		return pangoolConfig;
  	}
  	  	
  	/**
  	 * Return the Hadoop {@link ReduceContext}.  
  	 */
  	public ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object> getHadoopContext() {
  		return hadoopContext;
  	}
  }  
}
