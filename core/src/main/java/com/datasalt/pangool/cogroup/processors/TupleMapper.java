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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangool.cogroup.TupleMRConfig;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.MultipleOutputsCollector;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ITuple;

/**
 * TODO doc
 */
@SuppressWarnings({ "rawtypes", "serial" })
public abstract class TupleMapper<INPUT_KEY, INPUT_VALUE> extends
    Mapper<INPUT_KEY, INPUT_VALUE, DatumWrapper<ITuple>, NullWritable> implements Serializable {
  
	private Collector collector;
	private TupleMRContext context;

	/**
	 * Called once at the start of the task. Override it to implement your custom logic.
	 */
	public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {

	}

	/**
	 * Called once at the end of the task. Override it to implement your custom logic.
	 */
	public void cleanup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
		
	}

	/**
	 * Called once per each input pair of key/values. Override it to implement your custom logic.
	 */
	public abstract void map(INPUT_KEY key, INPUT_VALUE value, TupleMRContext context, Collector collector)
	    throws IOException, InterruptedException;
	
	/**
	 * Do not override. Override {@link TupleMapper#setup(Collector)} instead.
	 */
	@Override
	public final void setup(Mapper<INPUT_KEY, INPUT_VALUE, DatumWrapper<ITuple>, NullWritable>.Context context) throws IOException, InterruptedException {
		try {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			TupleMRConfig pangoolConfig = TupleMRConfig.get(conf);
			this.context = new TupleMRContext(context, pangoolConfig);
			this.collector = new Collector(context);
			setup(this.context, this.collector);
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Do not override. Override {@link TupleMapper#cleanup(Collector)} instead.
	 */
	@Override
	public final void cleanup(Context context) throws IOException, InterruptedException {
		cleanup(this.context, collector);
		collector.close();
		super.cleanup(context);
	}

	/**
	 * Do not override! Override {@link TupleMapper#process(Object, Object, Collector)} instead.
	 */
	@Override
	public final void map(INPUT_KEY key, INPUT_VALUE value, Context context) throws IOException, InterruptedException {
		map(key, value, this.context, collector);
	}	
	
	/* ------------ INNER CLASSES ------------ */
	
	/**
	 * Class for collecting data inside a {@link TupleMapper}.
	 */
	public static class Collector extends MultipleOutputsCollector {

		private Mapper.Context context;
		
		private ThreadLocal<DatumWrapper<ITuple>> cachedDatum = new ThreadLocal<DatumWrapper<ITuple>>(){
			@Override
			public DatumWrapper<ITuple> get(){
				return new DatumWrapper<ITuple>();
			}
		};
			
		private NullWritable nullWritable;
		
		Collector(Mapper.Context context) {
			super(context);
			this.context = context;
			nullWritable = NullWritable.get();
		}

		@SuppressWarnings("unchecked")
    public void write(ITuple tuple) throws IOException, InterruptedException {
			DatumWrapper<ITuple> outputDatum = cachedDatum.get();
			outputDatum.datum(tuple);
			context.write(outputDatum, nullWritable);
		}
	}
	
	public static class StaticTupleMRContext<INPUT_KEY, INPUT_VALUE> {

		private MapContext<INPUT_KEY, INPUT_VALUE, DatumWrapper<ITuple>, NullWritable> context;
		private TupleMRConfig pangoolConfig;

		StaticTupleMRContext(MapContext<INPUT_KEY, INPUT_VALUE, DatumWrapper<ITuple>, NullWritable> context, TupleMRConfig pangoolConfig) {
			this.context = context;
			this.pangoolConfig = pangoolConfig;
		}

		/**
		 * Return the Hadoop {@link MapContext}.
		 */
		public MapContext<INPUT_KEY, INPUT_VALUE, DatumWrapper<ITuple>, NullWritable> getHadoopContext() {
			return context;
		}

		public TupleMRConfig getTupleMRConfig() {
			return pangoolConfig;
		}
	}
	
	public class TupleMRContext extends StaticTupleMRContext<INPUT_KEY, INPUT_VALUE> {
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the Collector meanwhile keeping generics. 
		 */
		TupleMRContext(MapContext<INPUT_KEY, INPUT_VALUE, DatumWrapper<ITuple>, NullWritable> context,
        TupleMRConfig pangoolConfig) {
	    super(context, pangoolConfig);
    }		
	}
}
