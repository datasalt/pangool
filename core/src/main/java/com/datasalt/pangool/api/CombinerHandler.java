package com.datasalt.pangool.api;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.api.GroupHandler.StaticCoGrouperContext;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.PangoolWrapper;

@SuppressWarnings("serial")
public class CombinerHandler implements Serializable {

	public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException, CoGrouperException {

	}

	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector) throws IOException,
  InterruptedException, CoGrouperException {

	}
	
	public void cleanup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}

	/* ------------ INNER CLASSES ------------ */	
	
	/**
	 * A class for collecting data inside a {@link CombinerHandler}.
	 * Warning: Not thread safe by default... If you want thread safe, TODO
	 */
	public static final class Collector {
		
    private ReduceContext<PangoolWrapper<ITuple>, NullWritable, PangoolWrapper<ITuple>, NullWritable> context;

    private PangoolWrapper<ITuple> cachedSourcedTuple = new PangoolWrapper<ITuple>();
    private NullWritable nullWritable = NullWritable.get();
    
		public Collector(CoGrouperConfig pangoolConfig, ReduceContext<PangoolWrapper<ITuple>, NullWritable, PangoolWrapper<ITuple>, NullWritable> context){
			this.context = context;
		}
		
    public void write(ITuple tuple) throws IOException,InterruptedException {
    	cachedSourcedTuple.currentDatum(tuple);
			context.write(cachedSourcedTuple, nullWritable);
		}
	}
  
  public class CoGrouperContext extends StaticCoGrouperContext<PangoolWrapper<ITuple>, NullWritable> {
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the CoGrouperContext meanwhile keeping generics. 
		 */
		public CoGrouperContext(ReduceContext<PangoolWrapper<ITuple>, NullWritable, PangoolWrapper<ITuple>, NullWritable> hadoopContext,
        CoGrouperConfig pangoolConfig) {
      super(hadoopContext, pangoolConfig);
    }    	
  }
}
