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

package com.datasalt.pangool.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.GroupHandler.CoGrouperContext;
import com.datasalt.pangool.api.GroupHandler.Collector;
import com.datasalt.pangool.commons.DCUtils;
import com.datasalt.pangool.io.tuple.FilteredReadOnlyTuple;
import com.datasalt.pangool.io.tuple.ITuple;

public class SimpleReducer<OUTPUT_KEY, OUTPUT_VALUE> extends Reducer<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	public final static String CONF_REDUCER_HANDLER = CoGrouper.class.getName() + ".reducer.handler";

	// Following variables protected to be shared by Combiners
	protected CoGrouperConfig pangoolConfig;
	protected Collector<OUTPUT_KEY, OUTPUT_VALUE> collector;
	protected TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	protected FilteredReadOnlyTuple groupTuple; // Tuple view over the group
	protected CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> context;

	private GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> handler;

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			this.pangoolConfig = CoGrouperConfigBuilder.get(context.getConfiguration());
			this.context = new CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE>(context, pangoolConfig);
			this.groupTuple = new FilteredReadOnlyTuple(pangoolConfig.getGroupByFields());
			this.collector = new Collector<OUTPUT_KEY, OUTPUT_VALUE>(context);

			this.grouperIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);

			loadHandler(context);

		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings({ "unchecked" })
	protected void loadHandler(Context context) throws IOException, InterruptedException, CoGrouperException {

		String fileName = context.getConfiguration().get(SimpleReducer.CONF_REDUCER_HANDLER);
		handler = DCUtils.loadSerializedObjectInDC(context.getConfiguration(), GroupHandler.class, fileName);
		if(handler instanceof Configurable) {
			((Configurable) handler).setConf(context.getConfiguration());
		}
		handler.setup(this.context, collector);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		cleanupHandler(context);
		collector.close();
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
	public final void reduce(ITuple key, Iterable<NullWritable> values, Context context) throws IOException,
	    InterruptedException {
		Iterator<NullWritable> iterator = values.iterator();
		grouperIterator.setIterator(iterator);

		// We get the firts tuple, to create the groupTuple view
		ITuple firstTupleGroup = key;

		// A view is created over the first tuple to give the user the group fields
		groupTuple.setDelegatedTuple(firstTupleGroup);
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
