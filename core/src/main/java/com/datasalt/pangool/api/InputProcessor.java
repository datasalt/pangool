/**
 * Copyright [2011] [Datasalt Systems S.L.]
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

package com.datasalt.pangool.api;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.io.tuple.ITuple;

/**
 * TODO doc
 */
@SuppressWarnings("rawtypes")
public abstract class InputProcessor<INPUT_KEY, INPUT_VALUE> extends
    Mapper<INPUT_KEY, INPUT_VALUE, DoubleBufferedTuple, NullWritable> implements Serializable {

	/**
   * 
   */
  private static final long serialVersionUID = 1L;
  
	private Collector collector;
	private CoGrouperContext context;

	public static final class Collector extends MultipleOutputsCollector {

		private Mapper.Context context;

		private ThreadLocal<DoubleBufferedTuple> cachedSourcedTuple = new ThreadLocal<DoubleBufferedTuple>() {

			@Override
			protected DoubleBufferedTuple initialValue() {
				return new DoubleBufferedTuple();
			}
		};

		Collector(Mapper.Context context) {
			super(context);
			this.context = context;
		}

		@SuppressWarnings("unchecked")
		public void write(ITuple tuple) throws IOException, InterruptedException {
			DoubleBufferedTuple sTuple = cachedSourcedTuple.get();
			sTuple.setContainedTuple(tuple);
			context.write(sTuple, NullWritable.get());
		}

		@SuppressWarnings("unchecked")
		public void write(int sourceId, ITuple tuple) throws IOException, InterruptedException {
			DoubleBufferedTuple sTuple = cachedSourcedTuple.get();
			sTuple.setContainedTuple(tuple);
			sTuple.setInt(Field.SOURCE_ID_FIELD_NAME, sourceId);
			context.write(sTuple, NullWritable.get());
		}
	}

	public static class CoGrouperContext {

		private Mapper.Context context;
		private CoGrouperConfig pangoolConfig;

		CoGrouperContext(Mapper.Context context, CoGrouperConfig pangoolConfig) {
			this.context = context;
			this.pangoolConfig = pangoolConfig;
		}

		/**
		 * Return the Hadoop {@link Mapper.Context}.
		 */
		public Mapper.Context getHadoopContext() {
			return context;
		}

		public CoGrouperConfig getPangoolConfig() {
			return pangoolConfig;
		}
	}

	/**
	 * Do not override. Override {@link InputProcessor#setup(Collector)} instead.
	 */
	@Override
	public final void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			CoGrouperConfig pangoolConfig = CoGrouperConfigBuilder.get(conf);
			this.context = new CoGrouperContext(context, pangoolConfig);
			this.collector = new Collector(context);
			setup(this.context, this.collector);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Called once at the start of the task. Override it to implement your custom logic.
	 */
	public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {

	}

	/**
	 * Do not override. Override {@link InputProcessor#cleanup(Collector)} instead.
	 */
	@Override
	public final void cleanup(Context context) throws IOException, InterruptedException {
		cleanup(this.context, collector);
		collector.close();
	}

	/**
	 * Called once at the end of the task. Override it to implement your custom logic.
	 */
	public void cleanup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
	}

	/**
	 * Do not override! Override {@link InputProcessor#process(Object, Object, Collector)} instead.
	 */
	@Override
	public final void map(INPUT_KEY key, INPUT_VALUE value, Context context) throws IOException, InterruptedException {
		process(key, value, this.context, collector);
	}

	/**
	 * Called once per each input pair of key/values. Override it to implement your custom logic.
	 */
	public abstract void process(INPUT_KEY key, INPUT_VALUE value, CoGrouperContext context, Collector collector)
	    throws IOException, InterruptedException;
}
