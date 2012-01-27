package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.api.GroupHandler.CoGrouperContext;

public class CombinerHandler {

  @SuppressWarnings("rawtypes")
	public static final class Collector extends MultipleOutputsCollector {
		
    private Reducer.Context context;

//    private ThreadLocal<Record> cachedSourcedTuple = new ThreadLocal<Record>() {
//
//    	@Override
//      protected Record initialValue() {
//	      return new Record();
//      }
//    };
    
		public Collector(CoGrouperConfig pangoolConfig, Reducer.Context context){
			super(context);
			this.context = context;
		}
		
		@SuppressWarnings("unchecked")
    public void write(GenericRecord tuple) throws IOException,InterruptedException {
			//DoubleBufferedTuple sTuple = cachedSourcedTuple.get();
			//sTuple.setContainedTuple(tuple);
			//TODO hacer transformacion pertinente
			
			context.write(tuple, NullWritable.get());
		}
		
		
	}
  
	public void setup(CoGrouperContext<AvroKey,AvroValue> context, Collector collector) throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(CoGrouperContext<AvroKey, AvroValue> context, Collector collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}

	public void onGroupElements(GenericRecord group, Iterable<GenericRecord> tuples, CoGrouperContext<AvroKey, AvroValue> context, Collector collector) throws IOException,
	    InterruptedException, CoGrouperException {
		
	}
}
