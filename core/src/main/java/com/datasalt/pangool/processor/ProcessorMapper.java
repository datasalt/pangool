package com.datasalt.pangool.processor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangolin.commons.HadoopUtils;

public class ProcessorMapper<I1, I2, O1, O2> extends Mapper<I1, I2, O1, O2> {

	ProcessorHandler<I1, I2, O1, O2> delegate;
	public final static String PROCESSOR_HANDLER = ProcessorMapper.class.getName() + ".processor.handler";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		/*
		 * Load serialized instance (delegate)
		 */
		Configuration conf = context.getConfiguration();
		Path path = HadoopUtils.locateFileInDC(conf, conf.get(PROCESSOR_HANDLER));
		ObjectInput in = new ObjectInputStream(new FileInputStream(new File(path + "")));
		try {
	    delegate = (ProcessorHandler)in.readObject();
    } catch(ClassNotFoundException e) {
	    throw new RuntimeException(e);
    }
		in.close();
		// -------------- //
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
