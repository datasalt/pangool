package com.datasalt.pangool.tuplemr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

	public MultipleInputsInterface(Configuration conf) {

	}

	private List<Input> multiInputs = new ArrayList<Input>();

	public static final class Input {

		Path path;
		InputFormat inputFormat;
		Mapper inputProcessor;

		Map<String, String> specificContext;

		Input(Path path, InputFormat inputFormat, Mapper inputProcessor, Map<String, String> specificContext) {
			this.path = path;
			this.inputFormat = inputFormat;
			this.inputProcessor = inputProcessor;
			this.specificContext = specificContext;
		}
	}

	/**
	 * Use this method for configuring a Job instance according to the multiple input specs that has been specified.
	 * Returns the instance files created.
	 */
	public Set<String> configureJob(Job job) throws FileNotFoundException, IOException {
		Set<String> instanceFiles = new HashSet<String>();
		for(Input input : getMultiInputs()) {
			instanceFiles.addAll(PangoolMultipleInputs.addInputPath(job, input.path, input.inputFormat,
			    input.inputProcessor, input.specificContext));
		}
		return instanceFiles;
	}

	public List<Input> getMultiInputs() {
		return multiInputs;
	}
}