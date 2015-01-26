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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class encapsulates the functionality of a builder such as
 * {@link TupleMRBuilder} that provides Multiple Inputs. To be used by other
 * builders like {@link MapOnlyJobBuilder}.
 */
@SuppressWarnings("rawtypes")
public class MultipleInputsInterface {

  public MultipleInputsInterface(Configuration conf) {

  }

  private Map<Path, List<Input>> multiInputs = Maps.newHashMap();
  private List<Input> allInputs = Lists.newArrayList();
  
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
   * Use this method for configuring a Job instance according to the multiple
   * input specs that has been specified. Returns the instance files created.
   */
  public Set<String> configureJob(Job job) throws FileNotFoundException, IOException {
    Set<String> instanceFiles = new HashSet<String>();
    for (Map.Entry<Path, List<Input>> entry : multiInputs.entrySet()) {
      for (int inputId = 0; inputId < entry.getValue().size(); inputId++) {
        Input input = entry.getValue().get(inputId);
        instanceFiles.addAll(PangoolMultipleInputs.addInputPath(job, input.path, input.inputFormat,
            input.inputProcessor, input.specificContext, inputId));
      }
    }
    return instanceFiles;
  }

  public void addInput(Input input) {
    List<Input> inputs = multiInputs.get(input.path);
    if (inputs == null) {
      inputs = new ArrayList<Input>();
      multiInputs.put(input.path, inputs);
    }
    inputs.add(input);
    allInputs.add(input);
  }
  
  public List<Input> getAllInputs() {
    return allInputs;
  }
}