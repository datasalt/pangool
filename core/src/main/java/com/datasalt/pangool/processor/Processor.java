package com.datasalt.pangool.processor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.commons.DCUtils;

/**
 * The Processor is a simple Pangool primitive that executes map-only Jobs. You can implement {@link ProcessorHandler} for using it.
 * See {@link Grep} for an example. You can instantiate your handler with Serializable state.
 * 
 * @author pere
 *
 */
public class Processor {

	private Configuration conf;

	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private Class<? extends OutputFormat> outputFormat;
	private ProcessorHandler processorHandler;
	
	public final static String SERIALIZED_HANDLER_LOCAL_FILE = Processor.class.getName() + ".serialized.handler.dat";
	private String serializedHandlerLocalFile = SERIALIZED_HANDLER_LOCAL_FILE;
	
	public void setSerializedHandlerLocalFile(String serializedHandlerLocalFile) {
  	this.serializedHandlerLocalFile = serializedHandlerLocalFile;
  }

	private static final class Input {

		Path path;
		Class<? extends InputFormat> inputFormat;

		Input(Path path, Class<? extends InputFormat> inputFormat) {
			this.path = path;
			this.inputFormat = inputFormat;
		}
	}

	private Path outputPath;
	private List<Input> multiInputs = new ArrayList<Input>();

	public Processor setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
		return this;
	}

	public Processor addInput(Path path, Class<? extends InputFormat> inputFormat) {
		this.multiInputs.add(new Input(path, inputFormat));
		return this;
	}

	public Processor setOutput(Path outputPath, Class<? extends OutputFormat> outputFormat, Class<?> outputKeyClass,
	    Class<?> outputValueClass) {
		this.outputFormat = outputFormat;
		this.outputKeyClass = outputKeyClass;
		this.outputValueClass = outputValueClass;
		this.outputPath = outputPath;
		return this;
	}
	
	public Processor setHandler(ProcessorHandler processorHandler) {
		this.processorHandler = processorHandler;
		return this;
	}

	public Processor(Configuration conf) {
		this.conf = conf;
	}

	public Job createJob() throws IOException, CoGrouperException, URISyntaxException {

		// TODO Checks
		
		DCUtils.serializeToDC(processorHandler, serializedHandlerLocalFile, ProcessorMapper.PROCESSOR_HANDLER, conf);
		
		Job job = new Job(conf);
		job.setNumReduceTasks(0);

		job.setJarByClass((jarByClass != null) ? jarByClass : processorHandler.getClass());
		job.setOutputFormatClass(outputFormat);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		for(Input input : multiInputs) {
			MultipleInputs.addInputPath(job, input.path, input.inputFormat, ProcessorMapper.class);
		}
		return job;
	}
}
