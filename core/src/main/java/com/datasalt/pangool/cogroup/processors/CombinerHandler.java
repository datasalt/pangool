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
package com.datasalt.pangool.cogroup.processors;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangool.cogroup.CoGrouperConfig;
import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.cogroup.processors.GroupHandler.StaticCoGrouperContext;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.DatumWrapper;

@SuppressWarnings("serial")
public class CombinerHandler implements Serializable {

	public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException, CoGrouperException {

	}

	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector) throws IOException,
  InterruptedException, CoGrouperException {

	}
	
	public void cleanup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}

	/* ------------ INNER CLASSES ------------ */	
	
	/**
	 * A class for collecting data inside a {@link CombinerHandler}.
	 * Warning: Not thread safe by default... If you want thread safe, TODO
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
    
		public Collector(CoGrouperConfig pangoolConfig, ReduceContext<DatumWrapper<ITuple>, NullWritable, DatumWrapper<ITuple>, NullWritable> context){
			this.context = context;
		}
		
    public void write(ITuple tuple) throws IOException,InterruptedException {
    	DatumWrapper<ITuple> outputDatum = cachedDatum.get();
    	outputDatum.datum(tuple);
			context.write(outputDatum, nullWritable);
		}
	}
  
  public class CoGrouperContext extends StaticCoGrouperContext<DatumWrapper<ITuple>, NullWritable> {
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the CoGrouperContext meanwhile keeping generics. 
		 */
		public CoGrouperContext(ReduceContext<DatumWrapper<ITuple>, NullWritable, DatumWrapper<ITuple>, NullWritable> hadoopContext,
        CoGrouperConfig pangoolConfig) {
      super(hadoopContext, pangoolConfig);
    }    	
  }
}
