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

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.avrool.CoGrouper;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.api.CombinerHandler;
import com.datasalt.avrool.api.CombinerHandler.Collector;

public class SimpleCombiner extends SimpleReducer<Record, NullWritable> {

	private CombinerHandler handler;
	private Collector collector;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		collector = new Collector(pangoolConfig, context);
	}

	@Override
	protected void loadHandler(Configuration conf, Context context) throws IOException, InterruptedException,
	    CoGrouperException {
		Class<? extends CombinerHandler> handlerClass = CoGrouper.getCombinerHandler(conf);
		handler = ReflectionUtils.newInstance(handlerClass, conf);
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