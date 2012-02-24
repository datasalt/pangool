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

package com.datasalt.pangool.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasalt.pangool.cogroup.SerializationInfo;
import com.datasalt.pangool.cogroup.TupleMRConfig;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ViewTuple;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.utils.DCUtils;

public class SimpleReducer<OUTPUT_KEY, OUTPUT_VALUE> extends Reducer<DatumWrapper<ITuple>, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	public final static String CONF_REDUCER_HANDLER = SimpleReducer.class.getName() + ".reducer.handler";

	private final static Logger log = LoggerFactory.getLogger(SimpleReducer.class);
	
	// Following variables protected to be shared by Combiners
	private TupleMRConfig grouperConfig;
	private SerializationInfo serInfo;
	private boolean isMultipleSources;
	private TupleReducer<OUTPUT_KEY, OUTPUT_VALUE>.Collector collector;
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	private ViewTuple groupTuple; // Tuple view over the group
	private TupleReducer<OUTPUT_KEY, OUTPUT_VALUE>.TupleMRContext context;
	private TupleReducer<OUTPUT_KEY, OUTPUT_VALUE> handler;

	@SuppressWarnings("unchecked")
  public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			log.info("Getting CoGrouper grouperConf.");
			this.grouperConfig = TupleMRConfig.get(context.getConfiguration());
			this.isMultipleSources = grouperConfig.getNumIntermediateSchemas() >= 2;
			this.serInfo = grouperConfig.getSerializationInfo();
			log.info("Getting CoGrouper grouperConf done.");
			if (!isMultipleSources){
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema(),this.serInfo.getGroupSchemaIndexTranslation(0)); //by default translation for 0
			} else {
				this.groupTuple = new ViewTuple(serInfo.getGroupSchema());
			}
			 
			this.grouperIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);
			
			// setting handler
			String fileName = context.getConfiguration().get(SimpleReducer.CONF_REDUCER_HANDLER);
			handler = DCUtils.loadSerializedObjectInDC(context.getConfiguration(), TupleReducer.class, fileName);
			if(handler instanceof Configurable) {
				((Configurable) handler).setConf(context.getConfiguration());
			}
			
			this.collector = handler.new Collector(context);
			this.context = handler.new TupleMRContext(context, grouperConfig);
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
	public final void reduce(DatumWrapper<ITuple> key, Iterable<NullWritable> values, Context context) throws IOException,	
	    InterruptedException {
		try {
			Iterator<NullWritable> iterator = values.iterator();
			grouperIterator.setIterator(iterator);
	
			// We get the firts tuple, to create the groupTuple view
			ITuple firstTupleGroup = key.datum();
	
			// A view is created over the first tuple to give the user the group fields
			if (isMultipleSources){
				int sourceId = grouperConfig.getSchemaIdByName(firstTupleGroup.getSchema().getName());
				int[] indexTranslation = serInfo.getGroupSchemaIndexTranslation(sourceId);
				groupTuple.setContained(firstTupleGroup,indexTranslation);
			} else {
				groupTuple.setContained(firstTupleGroup);
			}
			handler.reduce(groupTuple, grouperIterator, this.context, collector);
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}
}
