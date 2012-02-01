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
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;


import com.datasalt.avrool.CoGrouper;
import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.PangoolKey;
import com.datasalt.avrool.SerializationInfo;

import com.datasalt.avrool.api.GroupHandlerWithRollup;
import com.datasalt.avrool.api.GroupHandler.CoGrouperContext;
import com.datasalt.avrool.api.GroupHandler.Collector;
import com.datasalt.avrool.io.records.FilterRecord;


/**
 * 
 * This {@link Reducer} implements a similar functionality than {@link SimpleReducer} but adding a Rollup feature.
 */
public class RollupReducer<OUTPUT_KEY, OUTPUT_VALUE> extends Reducer<PangoolKey, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	private boolean firstIteration = true;
	private CoGrouperConfig grouperConfig;
	private CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> context;
	private Collector<OUTPUT_KEY, OUTPUT_VALUE> collector;
	private Schema commonSchema;
	private List<String> groupByFields;
	private int minDepth, maxDepth;
	private FilterRecord groupTuple;
	private RecordIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	private GroupHandlerWithRollup<OUTPUT_KEY, OUTPUT_VALUE> handler;
	private int[] identityMapping;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			this.grouperConfig = CoGrouperConfig.get(conf);
			SerializationInfo serInfo = SerializationInfo.get(grouperConfig);
			//this.commonSchema = this.pangoolConfig.getCommonOrderedSchema();
			this.context = new CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE>(context, grouperConfig);
			this.groupTuple = new FilterRecord(serInfo.getGroupSchema());
			this.groupByFields = grouperConfig.getGroupByFields();

			List<String> groupFields = grouperConfig.getGroupByFields();
			this.maxDepth = groupFields.size() - 1;
			this.minDepth = serInfo.getPartitionerSchema().getFields().size() - 1;

			this.grouperIterator = new RecordIterator<OUTPUT_KEY, OUTPUT_VALUE>(context, grouperConfig);
			this.collector = new Collector<OUTPUT_KEY, OUTPUT_VALUE>(context);
			this.identityMapping = SerializationInfo.getIdentityArray(this.grouperConfig.getGroupByFields().size());

			loadHandler(conf);
			handler.setup(this.context, collector);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	protected void loadHandler(Configuration conf) {
		Class<? extends GroupHandlerWithRollup<OUTPUT_KEY, OUTPUT_VALUE>> handlerClass = (Class<? extends GroupHandlerWithRollup<OUTPUT_KEY, OUTPUT_VALUE>>) CoGrouper
		    .getGroupHandler(conf);
		this.handler = ReflectionUtils.newInstance(handlerClass, conf);
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		try {
			handler.cleanup(this.context, collector);
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
				handler.onCloseGroup(i, groupByFields.get(i), (GenericRecord)context.getCurrentKey().datum(), this.context, collector);
			}
			cleanup(context);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void reduce(PangoolKey key, Iterable<NullWritable> values, Context context) throws IOException,
	    InterruptedException {
		try {
			Iterator<NullWritable> iterator = values.iterator();
			grouperIterator.setIterator(iterator);
			
			GenericRecord currentRecord = (GenericRecord)key.datum();
			int indexMismatch;
			if(firstIteration) {
				indexMismatch = minDepth;
				firstIteration = false;
			} else {
				GenericRecord previousKey = (GenericRecord)key.previousDatum();
				indexMismatch = indexMismatch(previousKey, currentRecord, minDepth, maxDepth);
				for(int i = maxDepth; i >= indexMismatch; i--) {
					handler.onCloseGroup(i, groupByFields.get(i), previousKey, this.context, collector);
				}
			}

			for(int i = indexMismatch; i <= maxDepth; i++) {
				handler.onOpenGroup(i, groupByFields.get(i), currentRecord, this.context, collector);
			}

			// we consumed the first element , so needs to comunicate to iterator
			//grouperIterator.setFirstTupleConsumed(true);

			// We set a view over the group fields to the method.
			groupTuple.setContained(currentRecord,identityMapping);

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
	private int indexMismatch(GenericRecord tuple1, GenericRecord tuple2, int minFieldIndex, int maxFieldIndex) {
		int i=0;
		for (Field f : commonSchema.getFields()){
			String name = f.name();
			if (tuple1.get(name).equals(tuple2.get(name))){
				return i;
			}
			i++;
		}
		
		return -1;
	}
}
