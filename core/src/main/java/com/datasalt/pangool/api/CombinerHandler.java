package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.api.GroupHandler.State;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.io.tuple.ITuple;

public class CombinerHandler {

	public static final class Collector {
		
    private Reducer.Context context;
    private PangoolConfig pangoolConfig;
    
    private ThreadLocal<DoubleBufferedTuple> cachedSourcedTuple = new ThreadLocal<DoubleBufferedTuple>() {

    	@Override
      protected DoubleBufferedTuple initialValue() {
	      return new DoubleBufferedTuple();
      }
    };
    
		public Collector(PangoolConfig pangoolConfig, Reducer.Context context){
			this.pangoolConfig = pangoolConfig;
			this.context = context;
		}
		
		/**
		 * Return the Hadoop {@link Mapper.Context}.  
		 */
		public Reducer.Context getHadoopContext() {
			return context;
		}
		
		public PangoolConfig getPangoolConfig() {
			return pangoolConfig;
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
	
	public void setup(State state, Reducer.Context context) throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(State state, Reducer.Context context) throws IOException, InterruptedException,
	    CoGrouperException {

	}

	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, Collector collector) throws IOException,
	    InterruptedException, CoGrouperException {
		
	}
}
