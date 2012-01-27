package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit;

import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.commons.DCUtils;

/**
 * This Mapper uses serialized {@link InputProcessor} instances as delegates. It reads the appropriate configuration
 * in the Hadoop Configuration and deserializes the needed instance for the current input split.
 * 
 * @author pere
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DelegatingPangoolMapper extends Mapper {

	public final static String SPLIT_MAPPING_CONF = DelegatingPangoolMapper.class.getName() + ".split.mapping.";
  InputProcessor delegate; // The delegate
	
	protected void setup(Context context) throws IOException ,InterruptedException {
		// Find out what delegate we need according to the Input Split
		InputSplit split = context.getInputSplit();
		if(context.getInputSplit() instanceof TaggedInputSplit) {
			split = ((TaggedInputSplit)context.getInputSplit()).getInputSplit();
		}
		if(!(split instanceof FileSplit)) {
			throw new RuntimeException("Incompatible input configuration with " + DelegatingPangoolMapper.class.getName() + ". Found split class: " + split.getClass() + ", expecting " + FileSplit.class.getName()); 
		}
		FileSplit fileSplit = (FileSplit)split;
		String configurationKey = SPLIT_MAPPING_CONF + fileSplit.getPath();
		if(context.getConfiguration().get(configurationKey) == null) {
			throw new RuntimeException("No serialized " + InputProcessor.class.getName() + " instance for split: " + fileSplit.getPath() + ". Something went wrong configuring Multiple Inputs");
		}
		delegate = DCUtils.loadSerializedObjectInDC(context.getConfiguration(), InputProcessor.class, configurationKey);
		delegate.setup(context);
	};
	
	protected void map(Object key, Object value, Context context) throws IOException ,InterruptedException {
		delegate.map(key, value, context);
	};
	
	protected void cleanup(Context context) throws IOException ,InterruptedException {
		delegate.cleanup(context);
	};
}
