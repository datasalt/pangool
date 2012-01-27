package com.datasalt.pangool.processor;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangool.commons.DCUtils;

public class ProcessorMapper<I1, I2, O1, O2> extends Mapper<I1, I2, O1, O2> {

	ProcessorHandler<I1, I2, O1, O2> delegate;
	public final static String PROCESSOR_HANDLER = ProcessorMapper.class.getName() + ".processor.handler";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Load serialized instance (delegate)
		delegate = DCUtils.loadSerializedObjectInDC(context.getConfiguration(), ProcessorHandler.class, PROCESSOR_HANDLER);
		delegate.setup(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		delegate.cleanup(context);
	}

	protected void map(I1 key, I2 value, Context context) throws IOException ,InterruptedException {
		delegate.map(key, value, context);
	};
}
