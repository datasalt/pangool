package com.datasalt.pangool.api;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.api.GroupHandler.StaticCoGrouperContext;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.io.tuple.ITuple;

@SuppressWarnings("serial")
public class CombinerHandler implements Serializable {

	public static final class Collector {
		
    private ReduceContext<ITuple, NullWritable, ITuple, NullWritable> context;

    private ThreadLocal<DoubleBufferedTuple> cachedSourcedTuple = new ThreadLocal<DoubleBufferedTuple>() {

    	@Override
      protected DoubleBufferedTuple initialValue() {
	      return new DoubleBufferedTuple();
      }
    };
    
		public Collector(CoGrouperConfig pangoolConfig, ReduceContext<ITuple, NullWritable, ITuple, NullWritable> context){
			this.context = context;
		}
		
    public void write(ITuple tuple) throws IOException,InterruptedException {
			DoubleBufferedTuple sTuple = cachedSourcedTuple.get();
			sTuple.setContainedTuple(tuple);
			context.write(sTuple, NullWritable.get());
		}
		
    public void write(int sourceId, ITuple tuple) throws IOException, InterruptedException {
			DoubleBufferedTuple sTuple = cachedSourcedTuple.get();
			sTuple.setContainedTuple(tuple);
			sTuple.setInt(Field.SOURCE_ID_FIELD_NAME, sourceId);		
			context.write(sTuple, NullWritable.get());
		}
	}
  
  public class CoGrouperContext extends StaticCoGrouperContext<ITuple, NullWritable> {
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the CoGrouperContext meanwhile keeping generics. 
		 */
		public CoGrouperContext(ReduceContext<ITuple, NullWritable, ITuple, NullWritable> hadoopContext,
        CoGrouperConfig pangoolConfig) {
      super(hadoopContext, pangoolConfig);
    }    	
  }

	public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}

	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector) throws IOException,
	    InterruptedException, CoGrouperException {
		
	}
}
