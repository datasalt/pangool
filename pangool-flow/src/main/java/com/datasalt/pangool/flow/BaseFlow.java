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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

import com.datasalt.pangool.utils.HadoopUtils;

/**
 * Base class for deriving flows. This class is low-level and not meant to be used directly.
 * <p>
 * Instead, users must use {@link MapReduceFlowBuilder}.
 */
@SuppressWarnings("serial")
public abstract class BaseFlow implements Serializable {

	private final transient List<String> inputs = new ArrayList<String>();
	private final transient Map<String, String> bindings = new HashMap<String, String>();
	private final transient Map<String, Step> jobContext = new HashMap<String, Step>();

	public void add(Step job) {
		if(jobContext.containsKey(job.getName())) {
			throw new IllegalArgumentException(job.getName() + " already binded to an instance of "
			    + jobContext.get(job.getName()).getClass().getName());
		}
		jobContext.put(job.getName(), job);
	}

	public void add(String input) {
		inputs.add(input);
	}

	public void bind(String name, Object bind) {
		if(bindings.get(name) != null) {
			throw new IllegalArgumentException("Property [" + name + "] already binded to "
			    + bindings.get(name));
		}
		bindings.put(name, bind.toString());
	}

	public Step findInOutputs(String output) {
		for(Step job : jobContext.values()) {
			if(output.equals(job.getOutputName())) {
				return job;
			}
			for(String namedOutput : job.getNamedOutputs()) {
				if(job.getNamedOutputName(namedOutput).equals(output)) {
					return job;
				}
			}
		}
		return null;
	}

	public static enum EXECUTION_MODE {
		OVERWRITE, POLITE
	}

	public void execute(final EXECUTION_MODE mode, final Configuration conf, String... outputs)
	    throws Exception {
		List<Step> toResolve = new ArrayList<Step>();

		for(String output : outputs) {
			Step orig = findInOutputs(output);
			if(orig == null) {
				throw new IllegalArgumentException("Unknown output: " + output + " not found in flow context.");
			}
			toResolve.add(orig);
		}

		Step orig;

		final Map<String, Step> jobOutputBindings = new HashMap<String, Step>();
		final Map<Step, Set<Step>> stepDependencies = new HashMap<Step, Set<Step>>();

		while(toResolve.size() > 0) {
			Iterator<Step> it = toResolve.iterator();
			orig = it.next();
			it.remove();
			Log.info("Resolving dependencies for " + orig.getName());

			Set<Step> deps = new HashSet<Step>();
			for(Input input : orig.getInputs()) {
				String inputName = orig.getName() + "." + input.name;
				String bindedTo = bindings.get(inputName);

				if(bindedTo == null) {
					throw new IllegalArgumentException("Input " + inputName
					    + " not binded to anything in current flow context.");
				}

				Step job = findInOutputs(bindedTo);
				if(job == null) {
					if(!inputs.contains(bindedTo)) {
						throw new IllegalArgumentException("Unknown input: " + bindedTo + " binded to " + inputName
						    + " not found in flow context.");
					}
				} else {
					deps.add(job);
					jobOutputBindings.put(inputName, job);
					toResolve.add(job);
				}
			}

			stepDependencies.put(orig, deps);
		}

		Log.info("Steps to execute and dependencies: " + stepDependencies);
		Set<Step> completedSteps = new HashSet<Step>();
		ExecutorService executor = Executors.newCachedThreadPool();
		Set<Future<Step>> stepsBeingExecuted = new HashSet<Future<Step>>();
		final AtomicBoolean flowFailed = new AtomicBoolean(false);

		while(stepDependencies.keySet().size() > 0) {
			// gather all steps at this level
			// steps to be executed at each moment are steps whose dependencies are only dependencies that already have been
			// executed
			Set<Step> stepsToExecuteInParallel = new HashSet<Step>();
			for(Map.Entry<Step, Set<Step>> entry : stepDependencies.entrySet()) {
				boolean canBeExecuted = true;
				for(Step dependencyStep : entry.getValue()) {
					if(!completedSteps.contains(dependencyStep)) {
						canBeExecuted = false;
						break;
					}
				}
				if(canBeExecuted) {
					stepsToExecuteInParallel.add(entry.getKey());
				}
			}

			if(stepsToExecuteInParallel.size() > 0) {
				Log.info("Launching parallel steps [" + stepsToExecuteInParallel + "]");
				
				for(final Step job : stepsToExecuteInParallel) {
					stepsBeingExecuted.add(executor.submit(new Runnable() {
						@Override
	          public void run() {
							try {
								List<String> args = new ArrayList<String>();
								for(Param param : job.getParameters()) {
									String paramName = job.getName() + "." + param.getName();
									args.add("-D");
									Object val = bindings.get(paramName);
									if(val == null) {
										val = conf.get(paramName);
										if(val == null) {
											throw new RuntimeException("Unresolved parameter: " + paramName
											    + " not present in bindings or Hadoop conf.");
										}
									}
									args.add(paramName + "=" + val);
								}
								for(Input input : job.getInputs()) {
									String inputName = job.getName() + "." + input.name;
									args.add("--" + input.name);
									String bindedTo = bindings.get(inputName);
									Step jOutput = jobOutputBindings.get(inputName);
									if(jOutput != null) {
										// sometimes we need to rewrite the path expression to avoid conflicts
										if(jOutput.namedOutputs.size() > 0) {
											if(bindedTo.endsWith(".output")) { // main output of a named output job
												// rebind to glob expression
												bindedTo = bindedTo + "/part*";
											} else { // a named output
												// rebind to glob expression
												int lastPoint = bindedTo.lastIndexOf(".");
												String namedOutput = bindedTo.substring(lastPoint + 1, bindedTo.length());
												bindedTo = bindedTo.substring(0, lastPoint) + "/" + namedOutput;
											}
										}
									}
									args.add(bindedTo);
								}
								args.add("--output");
								// Output = outputName if it's not binded
								String bindedTo = bindings.get(job.getOutputName());
								if(bindedTo == null) {
									bindedTo = job.getOutputName();
								}
								args.add(bindedTo);
								if(mode.equals(EXECUTION_MODE.OVERWRITE)) {
									Path p = new Path(bindedTo);
									HadoopUtils.deleteIfExists(p.getFileSystem(conf), p);
								}
								Log.info("Executing [" + job.getName() + "], args: " + args);
								if(ToolRunner.run(conf, job, args.toArray(new String[0])) < 0) {
									throw new RuntimeException("Flow failed at step [" + job.getName() + "]");
								}
							} catch(Throwable t) {
								t.printStackTrace();
								flowFailed.set(true);
							}
	          }
					}, job));
					stepDependencies.remove(job);
				}
			}
			
			// Wait until some job finishes, whichever one
			Set<Future<Step>> stepsThatFinished = new HashSet<Future<Step>>();

			while(stepsThatFinished.size() == 0) {
				Thread.sleep(1000);

				if(flowFailed.get()) {
					throw new RuntimeException("Flow failed!");				
				}

				for(Future<Step> stepBeingExecuted: stepsBeingExecuted) {
					if(stepBeingExecuted.isDone()) {
						Step doneStep = stepBeingExecuted.get();
						Log.info("Step done: [" + doneStep + "]");
						completedSteps.add(doneStep);
						stepsThatFinished.add(stepBeingExecuted);
					}
				}
				
				stepsBeingExecuted.removeAll(stepsThatFinished);
			};
		}
		
		// Wait until everything is finished
		// This is not very DRY - can it be improved?
		Set<Future<Step>> stepsThatFinished = new HashSet<Future<Step>>();

		while(stepsBeingExecuted.size() > 0) {
			Thread.sleep(1000);

			if(flowFailed.get()) {
				throw new RuntimeException("Flow failed!");				
			}

			for(Future<Step> stepBeingExecuted: stepsBeingExecuted) {
				if(stepBeingExecuted.isDone()) {
					Step doneStep = stepBeingExecuted.get();
					Log.info("Step done: [" + doneStep + "]");
					stepsThatFinished.add(stepBeingExecuted);
				}
			}
			
			stepsBeingExecuted.removeAll(stepsThatFinished);
		};
	}
}
