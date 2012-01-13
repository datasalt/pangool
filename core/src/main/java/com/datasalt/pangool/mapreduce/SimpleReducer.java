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

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.TupleIterator;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.tuple.FilteredReadOnlyTuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.mapreduce.GroupHandler.State;

/**
 * TODO
 * @author eric
 *
 */
public class SimpleReducer<OUTPUT_KEY,OUTPUT_VALUE> extends Reducer<ITuple, NullWritable, OUTPUT_KEY,OUTPUT_VALUE> {

	private PangoolConfig pangoolConfig;
	private State state;	
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> grouperIterator;
	private GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> handler;
	private FilteredReadOnlyTuple groupTuple; // Tuple view over the group
	
  @SuppressWarnings({"unchecked","rawtypes"})
  public void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		try {
			Configuration conf = context.getConfiguration();
			this.pangoolConfig = PangoolConfigBuilder.get(conf);
			this.state = new State(pangoolConfig);
			this.groupTuple = new FilteredReadOnlyTuple(pangoolConfig.getGroupByFields());

			this.grouperIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);
			Class<? extends GroupHandler> handlerClass = CoGrouper.getGroupHandler(conf);

			this.handler = ReflectionUtils.newInstance(handlerClass, conf);

			handler.setup(state, context);
			
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException,InterruptedException {
  	try{
  		super.cleanup(context);
  		handler.cleanup(state,context);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
    }
  }
  

  @Override
  public final void run(Context context) throws IOException,InterruptedException {
  	super.run(context);
  }
  
  @Override
	public final void reduce(ITuple key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		Iterator<NullWritable> iterator = values.iterator();
		grouperIterator.setIterator(iterator);
		
		// We get the firts tuple, to create the groupTuple view
		iterator.next();
		Tuple firstTupleGroup = (Tuple) context.getCurrentKey();

		// we consumed the first element , so needs to comunicate to iterator
		grouperIterator.setFirstTupleConsumed(true);
		
		// A view is created over the first tuple to give the user the group fields
		groupTuple.setDelegatedTuple(firstTupleGroup);
		
		try{
			handler.onGroupElements(groupTuple, grouperIterator, state, context);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
    }
	}
  
}
