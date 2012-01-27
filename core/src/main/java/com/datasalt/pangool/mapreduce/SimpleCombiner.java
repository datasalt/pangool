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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.api.CombinerHandler;
import com.datasalt.pangool.api.CombinerHandler.Collector;
import com.datasalt.pangool.commons.DCUtils;
import com.datasalt.pangool.io.tuple.ITuple;

public class SimpleCombiner extends SimpleReducer<ITuple, NullWritable> {

	public final static String CONF_COMBINER_HANDLER = CoGrouper.class.getName() + ".combiner.handler";

	private CombinerHandler handler;
	private Collector collector;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		collector = new Collector(pangoolConfig, context);
	}

	@Override
	protected void loadHandler(Context context) throws IOException, InterruptedException,
	    CoGrouperException {
		
		String fileName = context.getConfiguration().get(SimpleCombiner.CONF_COMBINER_HANDLER);
		handler = DCUtils.loadSerializedObjectInDC(context.getConfiguration(), CombinerHandler.class, fileName);
		if(handler instanceof Configurable) {
			((Configurable) handler).setConf(context.getConfiguration());
		}
		handler.setup(this.context, collector);
	}

	protected void callHandler(Context context) throws IOException, InterruptedException {
		try {
			handler.onGroupElements(groupTuple, grouperIterator, this.context, collector);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	};

	protected void cleanupHandler(Context context) throws IOException, InterruptedException {
		try {
			handler.cleanup(this.context, collector);
		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}
	};
}