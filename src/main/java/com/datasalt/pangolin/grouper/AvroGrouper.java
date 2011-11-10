package com.datasalt.pangolin.grouper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AvroGrouper {
	/**
	 * TODO
	 * @author epalace
	 *
	 * @param <KEY_OUT>
	 * @param <VALUE_OUT>
	 */
	public static abstract class Reducer<KEY,VALUE,OUT> extends AvroReducer<KEY,VALUE,OUT> {

	    	//private int currentLevel=0;
	    	private Tuple previousKey=null;
	    	//private Tuple.Prefix currentPrefix = null;
	    	private int maxLevels=0;
	    	
	  
	    	@Override
	    	public void close(){
        //Subclasses can override this as desired.
	    	}
	    	
	   @Override
	   public void	configure(JobConf jobConf){
	  	 super.configure(jobConf);
	  	 
	  	 
	   }
	    	

		public void reduce(KEY key, Iterable<VALUE> values, AvroCollector<OUT> collector, Reporter reporter){
			
//			Iterator<NullWritable> iterator = values.iterator();
//			while (iterator.hasNext()){
//				iterator.next();
//				//AvroKey<String> avroKey = new AvroKey<String>();
//				
//				Tuple currentKey = context.getCurrentKey();
//				if (previousKey == null){
//					for (int i = 0 ; i < maxLevels; i++){
//						onOpenGroup(context,null); //TODO bad
//					}
//					onElement(currentKey,context);
//					previousKey = new Tuple();
//					previousKey.set(currentKey);
//					
//					
//				} else {
//					int levelMismatch = Tuple.compareLevels(currentKey,previousKey,maxLevels);
//					int numClosingGroups = maxLevels -levelMismatch;
//					for (int i = 0 ; i < numClosingGroups ; i++){
//						onCloseGroup(context,null); //TODO bad
//					}
//					for (int i=0 ; i < numClosingGroups; i++){
//						onOpenGroup(context,null);
//					}
//					onElement(currentKey, context);
//					previousKey.set(currentKey);
//				}
//			}
		}
		
		/**
		 * TODO
		 * @param context
		 * @param prefix
		 */
		protected abstract void onOpenGroup(Context context,Tuple.Prefix prefix);

		/**
		 * TODO
		 * @param context
		 * @param prefix
		 */
		protected abstract void onCloseGroup(Context context,Tuple.Prefix prefix);

		/**
		 * TODO
		 * @param tuple
		 * @param context
		 */
		protected abstract void onElement(Tuple tuple, Context context);

	}
	
	public void main(String[] args){
		
	}
	
	
	
}
