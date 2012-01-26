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

package com.datasalt.avrool.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.avrool.CoGrouper;
import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.api.GroupHandler;
import com.datasalt.avrool.api.GroupHandler.CoGrouperContext;
import com.datasalt.avrool.api.GroupHandler.Collector;

public class SimpleReducer<OUTPUT_KEY, OUTPUT_VALUE> extends Reducer<Record, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	// Following variables protected to be shared by Combiners
	protected CoGrouperConfig pangoolConfig;
	protected Collector<OUTPUT_KEY, OUTPUT_VALUE> collector;
	protected TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	protected Record groupTuple; // Tuple view over the group
	protected CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> context;

	private GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> handler;

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			Configuration conf = context.getConfiguration();
			this.pangoolConfig = CoGrouperConfig.get(conf);
			this.context = new CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE>(context, pangoolConfig);
			//TODO 
			//this.groupTuple = new FilteredReadOnlyTuple(pangoolConfig.getGroupByFields());
			this.collector = new Collector<OUTPUT_KEY, OUTPUT_VALUE>(context);

			this.grouperIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);

			loadHandler(conf, context);

		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
  protected void loadHandler(Configuration conf, Context context) throws IOException, InterruptedException,
	    CoGrouperException {
		Class<? extends GroupHandler> handlerClass = CoGrouper.getGroupHandler(conf);
		handler = ReflectionUtils.newInstance(handlerClass, conf);
		handler.setup(this.context, collector);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		cleanupHandler(context);
	}

	protected void cleanupHandler(Context context) throws IOException, InterruptedException {
		try {
			handler.cleanup(this.context, collector);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void run(Context context) throws IOException, InterruptedException {
		super.run(context);
	}

	@Override
	public final void reduce(Record key, Iterable<NullWritable> values, Context context) throws IOException,
	    InterruptedException {
		Iterator<NullWritable> iterator = values.iterator();
		grouperIterator.setIterator(iterator);

		// We get the firts tuple, to create the groupTuple view
		iterator.next();
		Record firstTupleGroup = (Record) context.getCurrentKey();

		// we consumed the first element , so needs to comunicate to iterator
		grouperIterator.setFirstTupleConsumed(true);

		// A view is created over the first tuple to give the user the group fields
		//groupTuple.setDelegatedTuple(firstTupleGroup);
		callHandler(context);
	}

	protected void callHandler(Context context) throws IOException, InterruptedException {
		try {
			handler.onGroupElements(groupTuple, grouperIterator, this.context, collector);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}
}
