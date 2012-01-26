package com.datasalt.avrool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Schema.Field;
import com.datasalt.avrool.api.GroupHandler.CoGrouperContext;
import com.datasalt.avrool.io.tuple.DoubleBufferedTuple;
import com.datasalt.avrool.io.tuple.ITuple;

public class CombinerHandler {

  @SuppressWarnings("rawtypes")
	public static final class Collector extends MultipleOutputsCollector {
		
    private Reducer.Context context;

    private ThreadLocal<DoubleBufferedTuple> cachedSourcedTuple = new ThreadLocal<DoubleBufferedTuple>() {

    	@Override
      protected DoubleBufferedTuple initialValue() {
	      return new DoubleBufferedTuple();
      }
    };
    
		public Collector(CoGrouperConfig pangoolConfig, Reducer.Context context){
			super(context);
			this.context = context;
		}
		
		@SuppressWarnings("unchecked")
    public void write(ITuple tuple) throws IOException,InterruptedException {
			DoubleBufferedTuple sTuple = cachedSourcedTuple.get();
			sTuple.setContainedTuple(tuple);
			context.write(sTuple, NullWritable.get());
		}
		
		@SuppressWarnings("unchecked")
    public void write(int sourceId, ITuple tuple) throws IOException, InterruptedException {
			DoubleBufferedTuple sTuple = cachedSourcedTuple.get();
			sTuple.setContainedTuple(tuple);
			sTuple.setInt(Field.SOURCE_ID_FIELD_NAME, sourceId);		
			context.write(sTuple, NullWritable.get());
		}
	}
  
	public void setup(CoGrouperContext<ITuple, NullWritable> context, Collector collector) throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(CoGrouperContext<ITuple, NullWritable> context, Collector collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}

	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext<ITuple, NullWritable> context, Collector collector) throws IOException,
	    InterruptedException, CoGrouperException {
		
	}
}
