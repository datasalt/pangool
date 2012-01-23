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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.GroupHandler.State;
import com.datasalt.pangool.io.tuple.FilteredReadOnlyTuple;
import com.datasalt.pangool.io.tuple.ITuple;

public class SimpleReducer<OUTPUT_KEY, OUTPUT_VALUE> extends Reducer<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	// Following variables protected to be shared by Combiners
	protected PangoolConfig pangoolConfig;
	protected TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	protected FilteredReadOnlyTuple groupTuple; // Tuple view over the group
	protected State state;

	private GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> handler;


	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			Configuration conf = context.getConfiguration();
			this.pangoolConfig = PangoolConfigBuilder.get(conf);
			this.state = new State(pangoolConfig);
			this.groupTuple = new FilteredReadOnlyTuple(pangoolConfig.getGroupByFields());

			this.grouperIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);

			loadHandler(conf, context);

		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	protected void loadHandler(Configuration conf, Context context) throws IOException, InterruptedException,
	    CoGrouperException {
		Class<? extends GroupHandler> handlerClass = CoGrouper.getGroupHandler(conf);
		handler = ReflectionUtils.newInstance(handlerClass, conf);
		handler.setup(state, context);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		cleanupHandler(context);
	}

	protected void cleanupHandler(Context context) throws IOException, InterruptedException {
		try {
			handler.cleanup(state, context);
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
		iterator.next();
		ITuple firstTupleGroup = (ITuple) context.getCurrentKey();

		// we consumed the first element , so needs to comunicate to iterator
		grouperIterator.setFirstTupleConsumed(true);

		// A view is created over the first tuple to give the user the group fields
		groupTuple.setDelegatedTuple(firstTupleGroup);
		callHandler(context);
	}

	protected void callHandler(Context context) throws IOException, InterruptedException {
		try {
			handler.onGroupElements(groupTuple, grouperIterator, state, context);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}
}
