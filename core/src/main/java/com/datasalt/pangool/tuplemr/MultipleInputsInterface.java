package com.datasalt.pangool.tuplemr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangool.tuplemr.mapred.lib.input.PangoolMultipleInputs;

/**
 * This class encapsulates the functionality of a builder such as {@link TupleMRBuilder} that provides Multiple Inputs.
 * To be used by other builders like {@link MapOnlyJobBuilder}.
 */
@SuppressWarnings("rawtypes")
public class MultipleInputsInterface {

	@SuppressWarnings("unused")
  private final Configuration conf;
	
	public MultipleInputsInterface(Configuration conf) {
		this.conf = conf;
	}
	
	private List<Input> multiInputs = new ArrayList<Input>();
	
	public static final class Input {

		Path path;
    InputFormat inputFormat;
		Mapper inputProcessor;

		Input(Path path, InputFormat inputFormat, Mapper inputProcessor) {
			this.path = path;
			this.inputFormat = inputFormat;
			this.inputProcessor = inputProcessor;
		}
	}

	/**
	 * Use this method for configuring a Job instance according to the multiple input specs that has been specified.
	 */
	public void configureJob(Job job) throws FileNotFoundException, IOException {
		for(Input input : getMultiInputs()) {
			PangoolMultipleInputs.addInputPath(job, input.path, input.inputFormat,
			    input.inputProcessor);
		}
	}
	
	public List<Input> getMultiInputs() {
  	return multiInputs;
  }
}