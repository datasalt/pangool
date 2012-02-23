/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.flow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.datasalt.pangool.cogroup.TupleMRBuilder;
import com.datasalt.pangool.cogroup.TupleMRException;

/**
 * This is the base class that executable jobs must extend for being used with {@link LinearFlow}.
 * <p>
 * Each job must declare in its constructor its input paths, named outputs and its input parameters with {@link Inputs},
 * {@link NamedOutputs} and {@link Params}.
 * <p>
 * Each job must have a unique name which is also specified in the constructor. The name is used for locating input /
 * output resources of a job. For instance, the output of a job is located as "name.output". These unique strings can
 * then be binded in {@link LinearFlow} for making connections between resources.
 * <p>
 * Jobs execute method {@link #run(Path, Map, Map)}. Parameters / input / output paths are parsed automatically and
 * passed as parameters of this method. Help and parameter error handling is handled transparently.
 */
public abstract class PangoolJob implements Configurable, Tool {

	protected String name;
	protected String help; // extra explanation on what this Job does
	protected List<String> inputs;
	protected List<String> namedOutputs;
	protected Params parameters;

	protected Configuration hadoopConf;

	public PangoolJob(String name, Inputs inputs) {
		this(name, inputs, Params.NONE, NamedOutputs.NONE, null);
	}

	public PangoolJob(String name, Inputs inputs, Params parameters) {
		this(name, inputs, parameters, NamedOutputs.NONE, null);
	}

	public PangoolJob(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs) {
		this(name, inputs, parameters, namedOutputs, null);
	}

	public PangoolJob(String name, Inputs inputs, Params parameters, NamedOutputs namedOutputs, String help) {
		this.name = name;
		this.help = help;
		this.inputs = inputs;
		this.namedOutputs = namedOutputs;
		this.parameters = parameters;
	}

	private String hadoopConfName(Param param) { // Same convention as input / output paths: name.resource
		return getName() + "." + param.getName();
	}

	private List<String> help() {
		List<String> helpLines = new ArrayList<String>();
		helpLines.add(name + (help == null ? "" : "(" + help + ")") + " - Usage:");
		helpLines.add("");
		helpLines.add("Parameters:");
		helpLines.add("");
		for(Param param : parameters) {
			helpLines.add("\t -D " + hadoopConfName(param) + " : " + param);
		}
		helpLines.add("");
		helpLines.add("Input paths:");
		helpLines.add("");
		for(String input : inputs) {
			helpLines.add("\t--" + input);
		}
		helpLines.add("");
		return helpLines;
	}

	@Override
	public int run(String[] args) throws Exception {
		Map<String, Path> parsedInputs = new HashMap<String, Path>();
		Map<String, Object> parsedParameters = new HashMap<String, Object>();
		Path outputPath = null;

		for(int i = 0; i < args.length; i++) {
			for(String input : inputs) {
				if(args[i].equals("-" + input) || args[i].equals("--" + input)) {
					parsedInputs.put(input, new Path(args[++i]));
					continue;
				}
			}
			if(args[i].equals("-output") || args[i].equals("--output")) {
				outputPath = new Path(args[++i]);
			}
		}

		List<String> errors = new ArrayList<String>();

		if(outputPath == null) {
			errors.add("Output path is mandatory. Use -output or --output for it.");
		}
		for(String input : inputs) {
			if(!parsedInputs.containsKey(input)) {
				errors.add("Missing required input path: [" + input + "]. Please add it with -" + input + " or --" + input);
			}
		}

		for(Param parameter : parameters) {
			String hadoopConfName = hadoopConfName(parameter);
			Object val = hadoopConf.get(hadoopConfName);
			if(val == null) {
				continue;
			}
			try {
				if(parameter.getClazz().equals(Integer.class)) {
					val = Integer.parseInt(val + "");
				} else if(parameter.getClazz().equals(Float.class)) {
					val = Float.parseFloat(val + "");
				} else if(parameter.getClazz().equals(Double.class)) {
					val = Double.parseDouble(val + "");
				} else if(parameter.getClazz().equals(String[].class)) { // support for list of strings
					val = hadoopConf.getStrings(hadoopConfName);
				} else if(parameter.getClazz().equals(Long.class)) {
					val = Long.parseLong(val + "");
				}
				parsedParameters.put(parameter.getName(), val);
			} catch(Throwable e) {
				e.printStackTrace();
				errors.add("Error when parsing parameter: " + parameter + " : " + e);
			}
		}

		for(Param param : parameters) {
			if(!parsedParameters.containsKey(param.getName())) {
				errors.add("Missing required parameter: [" + param + "]. Please add it with -D " + hadoopConfName(param));
			}
		}

		if(errors.size() > 0) {
			for(String error : errors) {
				System.err.println(error);
			}
			for(String help : help()) {
				System.out.println(help);
			}
			return -1;
		}

		return run(outputPath, parsedInputs, parsedParameters);
	}

	/**
	 * Method that Job implementations must implement for executing the job business logic. It receives all the necessary
	 * things already parsed.
	 */
	public abstract int run(Path outputPath, Map<String, Path> parsedInputs, Map<String, Object> parsedParameters)
	    throws Exception;

	/**
	 * Convenience method that can be used by Jobs for executing Pangool's {@link CoGrouper} instances.
	 */
	public int executeCoGrouper(TupleMRBuilder coGrouper) throws IOException, TupleMRException, InterruptedException,
	    ClassNotFoundException {

		Job job = coGrouper.createJob();
		if(job.waitForCompletion(true)) {
			return 1;
		}
		return -1;
	}

	public String getOutputName() {
		return name + "." + NamedOutputs.OUTPUT;
	}

	public String getNamedOutputName(String namedOutput) {
		return name + "." + namedOutput;
	}
	
	public String getName() {
		return name;
	}

	public List<String> getInputs() {
		return inputs;
	}

	public List<String> getNamedOutputs() {
		return namedOutputs;
	}

	public Params getParameters() {
		return parameters;
	}

	@Override
	public void setConf(Configuration hadoopConf) {
		this.hadoopConf = hadoopConf;
	}

	@Override
	public Configuration getConf() {
		return hadoopConf;
	}

	public String toString() {
		return name;
	}
}
