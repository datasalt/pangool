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

package com.datasalt.pangool.tuplemr.mapred;

import java.io.IOException;
import java.util.Iterator;

import com.datasalt.pangool.utils.InstancesDistributor;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.ViewTuple;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.TupleReducer.TupleMRContext;

/**
 * This is a proxy {@link Reducer} implementation that delegates its
 * funcionality to a {@link TupleReducer} instance configured by
 * {@link TupleMRBuilder#setTupleReducer(TupleReducer)}
 * 
 */
public class SimpleReducer<OUTPUT_KEY, OUTPUT_VALUE> extends
    Reducer<DatumWrapper<ITuple>, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	public final static String CONF_REDUCER_HANDLER = SimpleReducer.class.getName()
	    + ".reducer.handler";

	// Following variables protected to be shared by Combiners
	private TupleMRConfig tupleMRConfig;
	private SerializationInfo serInfo;
	private boolean isMultipleSources;
	private TupleReducer<OUTPUT_KEY, OUTPUT_VALUE>.Collector collector;
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> tupleIterator;
	private ViewTuple groupTuple; // Tuple view over the group
	private TupleMRContext context;
	private TupleReducer<OUTPUT_KEY, OUTPUT_VALUE> handler;

	@SuppressWarnings("unchecked")
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			this.tupleMRConfig = TupleMRConfig.get(context.getConfiguration());
			this.isMultipleSources = tupleMRConfig.getNumIntermediateSchemas() >= 2;
			this.serInfo = tupleMRConfig.getSerializationInfo();
			if(!isMultipleSources) {
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema(),
				    this.serInfo.getGroupSchemaIndexTranslation(0)); // by default
																														 // translation for
																														 // 0
			} else {
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema());
			}

			this.tupleIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);

			// setting handler
			String fileName = context.getConfiguration()
			    .get(SimpleReducer.CONF_REDUCER_HANDLER);
			handler = InstancesDistributor.loadInstance(context.getConfiguration(),
          TupleReducer.class, fileName, true);

			this.collector = handler.new Collector(
			    (ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object>) context);
			this.context = new TupleMRContext(
			    (ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object>) context,
			    tupleMRConfig);
			handler.setup(this.context, collector);

		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		try {
			handler.cleanup(this.context, collector);
			collector.close();
			super.cleanup(context);

		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void reduce(DatumWrapper<ITuple> key, Iterable<NullWritable> values,
	    Context context) throws IOException, InterruptedException {
		try {
			Iterator<NullWritable> iterator = values.iterator();
			tupleIterator.setIterator(iterator);

			// We get the firts tuple, to create the groupTuple view
			ITuple firstTupleGroup = key.datum();

			// A view is created over the first tuple to give the user the group
			// fields
			if(isMultipleSources) {
				int schemaId = tupleMRConfig.getSchemaIdByName(firstTupleGroup.getSchema()
				    .getName());
				int[] indexTranslation = serInfo.getGroupSchemaIndexTranslation(schemaId);
				groupTuple.setContained(firstTupleGroup, indexTranslation);
			} else {
				groupTuple.setContained(firstTupleGroup);
			}
			handler.reduce(groupTuple, tupleIterator, this.context, collector);
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}
}
