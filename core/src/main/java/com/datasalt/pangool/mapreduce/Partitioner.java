package com.datasalt.pangool.mapreduce;


import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SerializationInfo;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ITuple;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<DatumWrapper<ITuple>, NullWritable> implements Configurable {

	private CoGrouperConfig grouperConfig;
	private SerializationInfo serInfo;
	private Configuration conf;
	private final Text text = new Text(); //to perform hashCode of strings
	
	@Override
	public int getPartition(DatumWrapper<ITuple> key, NullWritable value, int numPartitions) {
		ITuple tuple = key.currentDatum();
		tuple.getSchema();
		//return Tuple.partialHashCode(tuple,groupFields.length) % numPartitions;
		//TODO
		return 0;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if(conf != null) {
			this.conf = conf;
			try {
				this.grouperConfig = CoGrouperConfig.get(conf);
			} catch (CoGrouperException e) {
				throw new RuntimeException(e);
			}
		}
	}

	
	
	/**
	 * Calculates a combinated hashCode using the specified number of fields.
	 * 
	 */
	public int partialHashCode(ITuple tuple,int[] fields) {
		int result = 0;
		for(int field : fields) {
			Object o = tuple.get(field);
			int hashCode;
			if (o instanceof String){ //since String.hashCode() != Text.hashCode()
				text.set((String)o);
				hashCode = text.hashCode();
			} else {
				hashCode = o.hashCode();
			}
			result = result * 31 + hashCode;
		}
		return result & Integer.MAX_VALUE;
	}
	
}