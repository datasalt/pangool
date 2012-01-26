package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
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
public class GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static final class Collector<OUTPUT_KEY, OUTPUT_VALUE> extends MultipleOutputsCollector {

		Reducer.Context context;
		
    public Collector(Reducer.Context context) {
	    super(context);
	    this.context = context;
    }
		
		public void write(OUTPUT_KEY key, OUTPUT_VALUE value) throws IOException, InterruptedException {
			context.write(key, value);
		}

		/**
		 * Return the Hadoop {@link Mapper.Context}.  
		 */
		public Reducer.Context getHadoopContext() {
			return context;
		}
	}
	
  public static class CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> {
  	
  	private CoGrouperConfig pangoolConfig;
  	private ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> hadoopContext;
  	
  	public CoGrouperContext(ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> hadoopContext, CoGrouperConfig pangoolConfig) {
  		this.pangoolConfig = pangoolConfig;
  		this.hadoopContext = hadoopContext;
  	}

  	public CoGrouperConfig getPangoolConfig() {
  		return pangoolConfig;
  	}
  	
  	/**
  	 * Return the Hadoop {@link ReduceContext}.  
  	 */
  	public ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> getHadoopContext() {
  		return hadoopContext;
  	}
  }
	
	public void setup(CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> pangoolContext, Collector<OUTPUT_KEY, OUTPUT_VALUE> collector)
	    throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> pangoolContext, Collector<OUTPUT_KEY, OUTPUT_VALUE> collector)
	    throws IOException, InterruptedException, CoGrouperException {
	}

	/**
	 * 
	 * This is method is called with an iterable that contains all the tuples that have been grouped by the fields defined
	 * in {@link Grouper#setFieldsToGroupBy(String...)}
	 * 
	 * @param tuples
	 *          Iterable that contains all the tuples from a group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 */
	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext<OUTPUT_KEY, OUTPUT_VALUE> pangoolContext, Collector<OUTPUT_KEY, OUTPUT_VALUE> collector) throws IOException, InterruptedException,
	    CoGrouperException {

	}
}
