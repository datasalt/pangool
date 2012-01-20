package com.datasalt.pangool.processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.mapreduce.GroupComparator;
import com.datasalt.pangool.mapreduce.Partitioner;
import com.datasalt.pangool.mapreduce.SortComparator;

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

	private void serializeToDC(ProcessorHandler handler, Configuration conf) throws FileNotFoundException, IOException, URISyntaxException {
		File file = new File(serializedHandlerLocalFile);
		ObjectOutput out = new ObjectOutputStream(new FileOutputStream(file));
		out.writeObject(handler);
		out.close();
		
		FileSystem fS = FileSystem.get(conf);
		
		Path toHdfs = new Path(file.toURI());
		if(!fS.equals(FileSystem.getLocal(conf))) {
			if(fS.exists(toHdfs)) { // Optionally, copy to DFS if
				fS.delete(toHdfs, true);
			}
			FileUtil.copy(FileSystem.getLocal(conf), toHdfs, FileSystem.get(conf), toHdfs, false, conf);
		}
		
		conf.set(ProcessorMapper.PROCESSOR_HANDLER, file + "");
		DistributedCache.addCacheFile(new URI(serializedHandlerLocalFile), conf);
	}

	public Job createJob() throws IOException, CoGrouperException, URISyntaxException {
		/*
		 * Checks and blah blah
		 */
		serializeToDC(processorHandler, conf);
		
		Job job = new Job(conf);
		job.setNumReduceTasks(0);

		job.setJarByClass((jarByClass != null) ? jarByClass : processorHandler.getClass());
		job.setOutputFormatClass(outputFormat);
		job.setMapOutputKeyClass(DoubleBufferedTuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(Partitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		for(Input input : multiInputs) {
			MultipleInputs.addInputPath(job, input.path, input.inputFormat, ProcessorMapper.class);
		}
		return job;
	}
}
