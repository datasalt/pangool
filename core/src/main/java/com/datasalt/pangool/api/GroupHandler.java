package com.datasalt.pangool.api;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.mapreduce.RollupReducer;
import com.datasalt.pangool.mapreduce.SimpleReducer;

/**
 * 
 * This is the common interface that any {@link CoGrouper} job needs to implement. This handler is called in the reducer
 * step by {@link SimpleReducer} or {@link RollupReducer} depending if Roll-up feature is used.
 * 
 * @author eric
 * 
 */
@SuppressWarnings("serial")
public class GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> implements Serializable {

	public static class StaticCollector<OUTPUT_KEY, OUTPUT_VALUE> extends MultipleOutputsCollector {

		ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context;
		
    public StaticCollector(ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context) {
	    super(context);
	    this.context = context;
    }
		
		public void write(OUTPUT_KEY key, OUTPUT_VALUE value) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public class Collector extends StaticCollector<OUTPUT_KEY, OUTPUT_VALUE> {
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the Collector meanwhile keeping generics. 
		 */
		public Collector(ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context) {
	    super(context);
    }		
	}
	
  public static class StaticCoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> {
  	
  	private CoGrouperConfig pangoolConfig;
  	private ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> hadoopContext;
  	
  	public StaticCoGrouperContext(ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> hadoopContext, CoGrouperConfig pangoolConfig) {
  		this.pangoolConfig = pangoolConfig;
  		this.hadoopContext = hadoopContext;
  	}

  	public CoGrouperConfig getCoGrouperConfig() {
  		return pangoolConfig;
  	}
  	  	
  	/**
  	 * Return the Hadoop {@link ReduceContext}.  
  	 */
  	public ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> getHadoopContext() {
  		return hadoopContext;
  	}
  }
  
  public class CoGrouperContext extends StaticCoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> {
		/*
		 * This non static inner class is created to eliminate the need in
		 * of the extended GroupHandler methods to specify the generic types
		 * for the CoGrouperContext meanwhile keeping generics. 
		 */
		public CoGrouperContext(ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> hadoopContext,
        CoGrouperConfig pangoolConfig) {
      super(hadoopContext, pangoolConfig);
    }    	
  }
	
	public void setup(CoGrouperContext coGrouperContext, Collector collector)
	    throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(CoGrouperContext coGrouperContext, Collector collector)
	    throws IOException, InterruptedException, CoGrouperException {
	}

	/**
	 * 
	 * This method is called with an iterable that contains all the tuples that have been grouped by the fields defined
	 * in {@link Grouper#setFieldsToGroupBy(String...)}
	 * 
	 * @param tuples
	 *          Iterable that contains all the tuples from a group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 */
	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext coGrouperContext, Collector collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}
}
