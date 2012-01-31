package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.PangoolKey;
import com.datasalt.avrool.api.GroupHandler.CoGrouperContext;
import com.datasalt.avrool.io.records.MapperProxyRecord;

public class CombinerHandler {

  @SuppressWarnings("rawtypes")
	public static final class Collector extends MultipleOutputsCollector {
		
    private Reducer.Context context;
    private CoGrouperConfig pangoolConfig;
    private PangoolKey<MapperProxyRecord> outputKey;//TODO wrong!!! this is not thread safe!!! just for tests
    
    
    private ThreadLocal<PangoolKey<MapperProxyRecord>> PANGOOL_KEY_FACTORY = new ThreadLocal<PangoolKey<MapperProxyRecord>>() {

			@Override
			protected PangoolKey<MapperProxyRecord> initialValue() {
				PangoolKey<MapperProxyRecord> result = new PangoolKey<MapperProxyRecord>();
				result.datum(new MapperProxyRecord(pangoolConfig));
				return result;
			}
		};
    
//  
		public Collector(CoGrouperConfig pangoolConfig, Reducer.Context context){
			super(context);
			this.context = context;
			this.pangoolConfig = pangoolConfig;
			this.outputKey = PANGOOL_KEY_FACTORY.get();//TODO wrong!!! this is not thread safe!!! just for tests
		}
		
		@SuppressWarnings("unchecked")
    public void write(GenericRecord tuple) throws IOException,InterruptedException {
			PangoolKey<MapperProxyRecord> outputKey = this.outputKey; //TODO wrong! this is not thread safe!!! just for tests purposes
			//PangoolKey outputKey = PANGOOL_KEY_FACTORY.get();
			MapperProxyRecord proxyRecord =  outputKey.datum();
			try{
				proxyRecord.setContainedRecord(tuple);
				context.write(outputKey, NullWritable.get());
			} catch(Exception e){
				throw new IOException(e);
			}
		}
	}
  
	public void setup(CoGrouperContext<PangoolKey,NullWritable> context, Collector collector) throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(CoGrouperContext<PangoolKey,NullWritable> context, Collector collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}

	public void onGroupElements(GenericRecord group, Iterable<GenericRecord> tuples, CoGrouperContext<PangoolKey,NullWritable> context, Collector collector) throws IOException,
	    InterruptedException, CoGrouperException {
		
	}
}
