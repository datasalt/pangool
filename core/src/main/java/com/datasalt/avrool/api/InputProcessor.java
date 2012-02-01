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

package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.PangoolKey;
import com.datasalt.avrool.SerializationInfo;
import com.datasalt.avrool.io.records.MapperProxyRecord;

/**
 * TODO doc
 */
@SuppressWarnings("rawtypes")
public abstract class InputProcessor<INPUT_KEY, INPUT_VALUE> extends
    Mapper<INPUT_KEY, INPUT_VALUE, Record, NullWritable> {

	private Collector collector;
	private CoGrouperContext context;

	public static final class Collector extends MultipleOutputsCollector {
		
		private Mapper.Context context;
		//private SerializationInfo serInfo;
		private CoGrouperConfig grouperConfig;
		private NullWritable outputValue = NullWritable.get();
		private PangoolKey<MapperProxyRecord> pangoolKeyInstance;//TODO wrong!!! this is not thread safe!!! just for tests
		
//		private ThreadLocal<MapperProxyRecord> mapperProxyRecord = new ThreadLocal<MapperProxyRecord>() {
//
//			@Override
//			protected MapperProxyRecord initialValue() {
//				return new MapperProxyRecord(grouperConfig);
//			}
//		};
		
		private ThreadLocal<PangoolKey<MapperProxyRecord>> FACTORY_PANGOOL_KEY = new ThreadLocal<PangoolKey<MapperProxyRecord>>() {

			@Override
			protected PangoolKey initialValue() {
				PangoolKey<MapperProxyRecord> result = new PangoolKey<MapperProxyRecord>();
				MapperProxyRecord proxy = new MapperProxyRecord(grouperConfig);
				result.datum(proxy);
				return result;
			}
		};
		

		Collector(Mapper.Context context) {
			super(context);
			this.context = context;
			try {
				this.grouperConfig = CoGrouperConfig.get(context.getConfiguration());
				//this.serInfo = SerializationInfo.get(config);
			} catch(CoGrouperException e){
				throw new RuntimeException(e); 
			}
			this.pangoolKeyInstance = FACTORY_PANGOOL_KEY.get(); //TODO wrong!!! this is not thread safe!!! just for tests
		}

		
		public void write(GenericRecord tuple) throws IOException, InterruptedException {
			//MapperProxyRecord outputRecord  = mapperProxyRecord.get();
			//PangoolKey outputKey = FACTORY_PANGOOL_KEY.get();
			PangoolKey<MapperProxyRecord> outputKey = pangoolKeyInstance; //TODO wrong! this is not thread safe!!! just for tests purposes
			MapperProxyRecord outputRecord = outputKey.datum();
			try{
				outputRecord.setContainedRecord(tuple);
			} catch(CoGrouperException e){
				throw new IOException(e); 
			}
			//PangoolKey outputKey = FACTORY_PANGOOL_KEY.get();
			//outputKey.datum(outputRecord);
			context.write(outputKey,outputValue); 
		}


	}

	public static class CoGrouperContext {
		
		private Mapper.Context context;
  	private CoGrouperConfig pangoolConfig;
  	
		CoGrouperContext(Mapper.Context context, CoGrouperConfig pangoolConfig) {
			this.context = context;
			this.pangoolConfig = pangoolConfig;
		}
		
		/**
		 * Return the Hadoop {@link Mapper.Context}.
		 */
		public Mapper.Context getHadoopContext() {
			return context;
		}
		
  	public CoGrouperConfig getPangoolConfig() {
  		return pangoolConfig;
  	}
	}
	
	/**
	 * Do not override. Override {@link InputProcessor#setup(Collector)} instead.
	 */
	@Override
	public final void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		try {
			Configuration conf = context.getConfiguration();
			CoGrouperConfig pangoolConfig = CoGrouperConfig.get(conf);
			
			this.context = new CoGrouperContext(context, pangoolConfig);
			this.collector = new Collector(context);
			setup(this.context, this.collector);

		} catch(CoGrouperException e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * Called once at the start of the task. Override it to implement your custom logic.
	 */
	public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {

	}

	/**
	 * Do not override. Override {@link InputProcessor#cleanup(Collector)} instead.
	 */
	@Override
	public final void cleanup(Context context) throws IOException, InterruptedException {
		cleanup(this.context, collector);
	}

	/**
	 * Called once at the end of the task. Override it to implement your custom logic.
	 */
	public void cleanup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
	}

	/**
	 * Do not override! Override {@link InputProcessor#process(Object, Object, Collector)} instead.
	 */
	@Override
	public final void map(INPUT_KEY key, INPUT_VALUE value, Context context) throws IOException, InterruptedException {
		process(key, value, this.context, collector);
	}

	/**
	 * Called once per each input pair of key/values. Override it to implement your custom logic.
	 */
	public abstract void process(INPUT_KEY key, INPUT_VALUE value, CoGrouperContext context, Collector collector) throws IOException,
	    InterruptedException;
}
