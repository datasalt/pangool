package com.datasalt.pangolin.mapred.joiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * Class for performing one-to-many joins. The user must implement
 * their own {@link OneSideMapper}, their own {@link ManySideMapper},
 * and their own {@link OneToManyReducer}.
 * <p>
 * The class OneToManyReducer has callbacks for receiving the tuples. 
 * It allows outer joins, inner joins and left/right joins.  
 * 
 * @author ivan
 */
@SuppressWarnings({ "rawtypes" })
public class JoinOneToMany {

	protected static final int ONE_CHANNEL = 0;
	protected static final int MANY_CHANNEL = 1;
	
	private MultiJoiner joiner;

	/**
	 * Override for include your own logic. Emit
	 * one side items using the emit() functions. But use those
	 * that doesn't have the channel as parameter.
	 *   
	 * @author ivan
	 */
	public static abstract class OneSideMapper<INPUT_KEY, INPUT_VALUE> extends MultiJoinMultiChannelMapper<INPUT_KEY, INPUT_VALUE> {
		
		protected void emit(String grouping, Object datum) throws IOException, InterruptedException {
			super.emit(grouping, datum, ONE_CHANNEL);
		}

		protected void emit(Object grouping, Object datum) throws IOException, InterruptedException {
			super.emit(grouping, datum, ONE_CHANNEL);
		}

		protected void emit(Text grouping, Object datum) throws IOException, InterruptedException {
			super.emit(grouping, datum, ONE_CHANNEL);
		}

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(String grouping, Object datum, int channel) throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + OneSideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");
    }

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Object grouping, Object datum, int channel) throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + OneSideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");    
		}

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Text grouping, WritableComparable secondarySort, Object datum, int channel) throws IOException,
    InterruptedException {
			throw new IOException("Unsuported method for " + OneSideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");    
		}

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Text grouping, Object datum, int channel) throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + OneSideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");
		}

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Object grouping, WritableComparable secondarySort, Object datum, int channel)
        throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + OneSideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");
		}		
	}
	
	/**
	 * Override for include your own logic. Emit
	 * many side items using the emit() functions. But use those
	 * that doesn't have the channel as parameter.
	 *   
	 * @author ivan
	 */
	public static abstract class ManySideMapper<INPUT_KEY, INPUT_VALUE> extends MultiJoinMultiChannelMapper<INPUT_KEY, INPUT_VALUE> {

		protected void emit(String grouping, Object datum) throws IOException, InterruptedException {
			super.emit(grouping, datum, MANY_CHANNEL);
		}

		protected void emit(Object grouping, Object datum) throws IOException, InterruptedException {
			super.emit(grouping, datum, MANY_CHANNEL);
		}

		protected void emit(Text grouping, Object datum) throws IOException, InterruptedException {
			super.emit(grouping, datum, MANY_CHANNEL);
		}
		
		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(String grouping, Object datum, int channel) throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + ManySideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");
    }

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Object grouping, Object datum, int channel) throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + ManySideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");    
		}

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Text grouping, WritableComparable secondarySort, Object datum, int channel) throws IOException,
    InterruptedException {
			throw new IOException("Unsuported method for " + ManySideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");    
		}

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Text grouping, Object datum, int channel) throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + ManySideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");
		}

		/**
		 * Unsupported. Use emit() methods without channel parameter instead. 
		 */
		@Override
    protected void emit(Object grouping, WritableComparable secondarySort, Object datum, int channel)
        throws IOException, InterruptedException {
			throw new IOException("Unsuported method for " + ManySideMapper.class.getSimpleName() + ". Use emit() methods without channel parameter instead");
		}		
	}
	
	/**
	 * A {@link MultiJoinReducer} that encapsulates 1 -to- Many joins. It keeps the first element in memory and calls a
	 * callback method for each (1, n) pair.
	 * 
	 * @author pere, ivan
	 * 
	 * @param <ONESIDE>
	 *          The class of the first element
	 * @param <MANYSIDE>
	 *          The class of the rest "n" elements. May be the same, or not.
	 * @param <KOUT>
	 *          The output key class.
	 * @param <VOUT>
	 *          The output value class.
	 */
	@SuppressWarnings({ "unchecked" })
	public static abstract class OneToManyReducer<ONESIDE, MANYSIDE, KOUT, VOUT> extends MultiJoinReducer<KOUT, VOUT> {

		public OneToManyReducer() {
		}

		/**
		 * Called when no object from the one side of the relation is present. 
		 * Ignore by default. Override to change behavior
		 */
		protected void onNoOneSideItem(Context ctx) throws IOException, InterruptedException { // only if firstClass != secondClass 
		}

		/**
		 * Called if there is more than one object of the one side of the relation. 
		 * Throws an exception by default. Override to change behavior  
		 */
		protected void onMoreThanOneSideItem(ONESIDE obj, Context ctx) throws IOException, InterruptedException { // only if firstClass != secondClass
			throw new IOException("More than one first class - only one item was expected");
		}
		
		/**
		 * Called when no object from the many side of the relation is present. 
		 * Ignore by default. Override to change behavior
		 */
		protected void onNoManySideItems(Context ctx) throws IOException, InterruptedException {
		}

		/**
		 * Called with the key of each group. Is the first call back method to be called.
		 * Ignore by default. Override to change behavior
		 */
		protected void onKey(MultiJoinPair key, Context ctx) throws IOException, InterruptedException {
		}

		/**
		 * Called with each pair generated for each relation. oneSideItem can be null
		 * if no one side item is present for this relation. manySideItem can be null
		 * if no many side items are present for this relation. 
		 */
		protected abstract void onPair(ONESIDE oneSideItem, MANYSIDE manySideItem, Context ctx) throws IOException,
		    InterruptedException;

		protected void reduce(MultiJoinPair key, Iterable<MultiJoinDatum<?>> value, Context ctx) throws IOException,
		    InterruptedException {

			boolean first = true;
			boolean second = false;
			ONESIDE firstValueInMemory = null;
			onKey(key, ctx);

			for(MultiJoinDatum datum : value) {
				if(first) {
					//
					first = false;
					Object obj = deserialize(datum);
					if (datum.getChannelId() != ONE_CHANNEL) {
						onNoOneSideItem(ctx);
						onPair(null, (MANYSIDE) obj, ctx);
						second = true;
						continue;
					} 
					firstValueInMemory = (ONESIDE) obj;
				} else {
					//
					if (datum.getChannelId() == ONE_CHANNEL) {
						Object obj = deserializeNewInstance(datum);
						onMoreThanOneSideItem((ONESIDE) obj, ctx);
						continue;
					}
					second = true;					
					Object obj = deserialize(datum);
					/*
					 * firstValueInMemory can be null at that point.
					 */
					onPair(firstValueInMemory, (MANYSIDE) obj, ctx);
				}
			}
			
			if(!second) {
				onNoManySideItems(ctx);
				/*
				 * At this point, firstValueInMemory cannot be null.
				 * But no second value is present, so it is null.
				 */
				onPair(firstValueInMemory, null, ctx);
			}
		};
	}
	
	public JoinOneToMany(String name, Configuration conf) {
		joiner = new MultiJoiner(name, conf);
	}

	public JoinOneToMany setReducer(Class<? extends OneToManyReducer> reducer) {
		joiner.setReducer(reducer);
		return this;
	}

	public JoinOneToMany setOutputKeyClass(Class outputKeyClass) {
		joiner.setOutputKeyClass(outputKeyClass);
		return this;
	}

	public JoinOneToMany setOutputValueClass(Class outputValueClass) {
		joiner.setOutputValueClass(outputValueClass);
		return this;
	}

	public JoinOneToMany setOutputFormat(Class<? extends OutputFormat> outputFormat) {
		joiner.setOutputFormat(outputFormat);
		return this;
	}

	public JoinOneToMany setOutputPath(Path outputPath) {
		joiner.setOutputPath(outputPath);
		return this;
	}	
	
	public JoinOneToMany setOneSideClass(Class<? extends Object> oneSideClass) throws IOException {
		joiner.setChannelDatumClass(ONE_CHANNEL, oneSideClass);
		return this;
	}
	
	public JoinOneToMany setManySideClass(Class<? extends Object> manySideClass) throws IOException {
		joiner.setChannelDatumClass(MANY_CHANNEL, manySideClass);
		return this;
	}
	
	public JoinOneToMany addOneSideInput(Path location, Class<? extends InputFormat> inputFormat,
	    Class<? extends OneSideMapper> mapper) throws IOException {
		joiner.addInput(location, inputFormat, mapper);
		return this;
	}
	
	public JoinOneToMany addManySideInput(Path location, Class<? extends InputFormat> inputFormat,
	    Class<? extends ManySideMapper> mapper) throws IOException {
		joiner.addInput(location, inputFormat, mapper);
		return this;
	}
	
	public Job getJob() throws IOException {
		return joiner.getJob();
	}

}
