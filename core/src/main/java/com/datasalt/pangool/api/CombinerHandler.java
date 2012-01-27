package com.datasalt.pangool.api;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.api.GroupHandler.CoGrouperContext;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.io.tuple.ITuple;

public class CombinerHandler implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	public static final class Collector {
		
    private Reducer.Context context;

    private ThreadLocal<DoubleBufferedTuple> cachedSourcedTuple = new ThreadLocal<DoubleBufferedTuple>() {

    	@Override
      protected DoubleBufferedTuple initialValue() {
	      return new DoubleBufferedTuple();
      }
    };
    
		public Collector(CoGrouperConfig pangoolConfig, Reducer.Context context){
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
