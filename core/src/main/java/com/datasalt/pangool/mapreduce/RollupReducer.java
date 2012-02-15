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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.SerializationInfo;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.GroupHandlerWithRollup;
import com.datasalt.pangool.commons.DCUtils;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ViewTuple;
import com.datasalt.pangool.io.tuple.ITuple;

/**
 * 
 * This {@link Reducer} implements a similar functionality than {@link SimpleReducer} but adding a Rollup feature.
 */
public class RollupReducer<OUTPUT_KEY, OUTPUT_VALUE> extends Reducer<DatumWrapper<ITuple>, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	private boolean firstIteration = true;
	private CoGrouperConfig grouperConfig;
	private SerializationInfo serInfo;
	private GroupHandler<OUTPUT_KEY, OUTPUT_VALUE>.CoGrouperContext context;
	private GroupHandler<OUTPUT_KEY, OUTPUT_VALUE>.Collector collector;
	private List<String> groupByFields;
	private int minDepth, maxDepth;
	private ViewTuple groupTuple;
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	private GroupHandlerWithRollup<OUTPUT_KEY, OUTPUT_VALUE> handler;
	private boolean isMultipleSources;

	@SuppressWarnings("unchecked")
  @Override
	public void setup(Context context) throws IOException, InterruptedException {
		try {
			this.grouperConfig = CoGrouperConfig.get(context.getConfiguration());
			this.isMultipleSources = this.grouperConfig.getNumSources() >=2;
			this.serInfo = grouperConfig.getSerializationInfo();
			if (!isMultipleSources){
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema(),this.serInfo.getGroupSchemaIndexTranslation(0)); //by default translation for 0
			} else {
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema()); 
			}
			this.groupByFields = grouperConfig.getGroupByFields();
			List<String> groupFields = grouperConfig.getGroupByFields();
			this.maxDepth = groupFields.size() - 1;
			this.minDepth = grouperConfig.calculateRollupBaseFields().size() - 1;
			this.grouperIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);

			String fileName = context.getConfiguration().get(SimpleReducer.CONF_REDUCER_HANDLER);
			handler = DCUtils.loadSerializedObjectInDC(context.getConfiguration(), GroupHandlerWithRollup.class, fileName);
			if(handler instanceof Configurable) {
				((Configurable) handler).setConf(context.getConfiguration());
			}
			collector = handler.new Collector(context);
			this.context = handler.new CoGrouperContext(context, grouperConfig);
			handler.setup(this.context, collector);
			
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		try {
			handler.cleanup(this.context, collector);
			collector.close();
			super.cleanup(context);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void run(Context context) throws IOException, InterruptedException {
		try {
			setup(context);
			firstIteration = true;
			while(context.nextKey()) {
				reduce(context.getCurrentKey(), context.getValues(), context);
				// TODO look if this matches super.run() implementation
			}

			// close last group
			for(int i = maxDepth; i >= minDepth; i--) {
				handler.onCloseGroup(i, groupByFields.get(i), context.getCurrentKey().datum(), this.context, collector);
			}
			cleanup(context);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void reduce(DatumWrapper<ITuple> key, Iterable<NullWritable> values, Context context) throws IOException,
	    InterruptedException {
		
		try {
			Iterator<NullWritable> iterator = values.iterator();
			grouperIterator.setIterator(iterator);
			ITuple currentTuple = key.datum();
			int indexMismatch;
			if(firstIteration) {
				indexMismatch = minDepth;
				firstIteration = false;
			} else {
				ITuple previousKey = key.previousDatum();
				indexMismatch = indexMismatch(previousKey, currentTuple, minDepth, maxDepth);
				for(int i = maxDepth; i >= indexMismatch; i--) {
					handler.onCloseGroup(i, groupByFields.get(i), previousKey, this.context, collector);
				}
			}

			for(int i = indexMismatch; i <= maxDepth; i++) {
				handler.onOpenGroup(i, groupByFields.get(i), currentTuple, this.context, collector);
			}

			// We set a view over the group fields to the method.
			if (isMultipleSources){ //TODO consider not using index translation for multiple sources  
				int sourceId = grouperConfig.getSourceIdByName(currentTuple.getSchema().getName());
				int[] indexTranslation = serInfo.getGroupSchemaIndexTranslation(sourceId);
				groupTuple.setContained(currentTuple,indexTranslation);
			} else {
				groupTuple.setContained(currentTuple);
			}

			handler.onGroupElements(groupTuple, grouperIterator, this.context, collector);

			// This loop consumes the remaining elements that reduce didn't consume
			// The goal of this is to correctly set the last element in the next onCloseGroup() call
			while(iterator.hasNext()) {
				iterator.next();
			}
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Compares sequentially the fields from two tuples and returns which field they differ. TODO: Use custom comparators
	 * when provided. The provided RawComparators must implements "compare" so we should use them.
	 * 
	 * @return
	 */
	private int indexMismatch(ITuple tuple1, ITuple tuple2, int minFieldIndex, int maxFieldIndex) {
		int sourceId1 = grouperConfig.getSourceIdByName(tuple1.getSchema().getName());
		int sourceId2 = grouperConfig.getSourceIdByName(tuple2.getSchema().getName());
		int[] translationTuple1 = serInfo.getGroupSchemaIndexTranslation(sourceId1);
		int[] translationTuple2 = serInfo.getGroupSchemaIndexTranslation(sourceId2);
		
		//TODO this is completely inconsistent with Custom comparators!!! FIXX
		for(int i = minFieldIndex; i <= maxFieldIndex; i++) {
			Object obj1 = tuple1.get(translationTuple1[i]);
			Object obj2 = tuple2.get(translationTuple2[i]);
			if(obj1 instanceof byte[]) {
				if(!Arrays.equals((byte[])obj1, (byte[])obj2)) { //TODO this not correct
					return i;
				}
			} else {
				if(!obj1.equals(obj2)) {
					return i;
				}
			}
		}
		return -1;
	}
}
