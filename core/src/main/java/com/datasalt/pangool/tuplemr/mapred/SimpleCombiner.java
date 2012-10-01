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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.ViewTuple;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.TupleReducer.TupleMRContext;
import com.datasalt.pangool.utils.DCUtils;

public class SimpleCombiner extends
    Reducer<DatumWrapper<ITuple>, NullWritable, DatumWrapper<ITuple>, NullWritable> {

	public final static String CONF_COMBINER_HANDLER = SimpleCombiner.class.getName()
	    + ".combiner.handler";

	// Following variables protected to be shared by Combiners
	private TupleMRConfig tupleMRConfig;
	private SerializationInfo serInfo;
	private TupleIterator<DatumWrapper<ITuple>, NullWritable> tupleIterator;
	private ViewTuple groupTuple; // Tuple view over the group
	private TupleMRContext context;
	private TupleReducer<ITuple, NullWritable> handler;
	private TupleReducer<ITuple, NullWritable>.CombinerCollector collector;
	private boolean isMultipleSources;

	@SuppressWarnings("unchecked")
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			this.tupleMRConfig = TupleMRConfig.get(context.getConfiguration());
			this.serInfo = this.tupleMRConfig.getSerializationInfo();
			this.isMultipleSources = this.tupleMRConfig.getNumIntermediateSchemas() >= 2;
			if(isMultipleSources) {
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema());
			} else {
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema(),
				    serInfo.getGroupSchemaIndexTranslation(0));
			}
			this.tupleIterator = new TupleIterator<DatumWrapper<ITuple>, NullWritable>(context);

			String fileName = context.getConfiguration().get(
			    SimpleCombiner.CONF_COMBINER_HANDLER);
			handler = DCUtils.loadSerializedObjectInDC(context.getConfiguration(),
			    TupleReducer.class, fileName, true);

			@SuppressWarnings("rawtypes")
			ReduceContext castedContext = context;
			this.context = new TupleMRContext(castedContext, tupleMRConfig);
			collector = handler.new CombinerCollector(castedContext);
			handler.setup(this.context, collector);
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		try {

			handler.cleanup(this.context, collector);
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