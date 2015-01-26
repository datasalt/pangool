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
package com.datasalt.pangool.tuplemr.mapred.lib.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.utils.InstancesDistributor;

/**
 * This class supports MapReduce jobs that have multiple input paths with a
 * different {@link InputFormat} and {@link Mapper} for each path.
 * <p>
 * This class is inspired by the
 * {@link org.apache.hadoop.mapred.lib.MultipleInputs}
 */
@SuppressWarnings("rawtypes")
public class PangoolMultipleInputs {

  public final static String PANGOOL_INPUT_DIR_FORMATS_PREFIX_CONF = "pangool.input.dir.formats.";
  public final static String PANGOOL_INPUT_DIR_MAPPERS_PREFIX_CONF = "pangool.input.dir.mappers.";

  private static final String MI_PREFIX = "pangool.inputs.input.";
  private static final String CONF = ".conf";

  /**
   * Add a {@link Path} with a custom {@link InputFormat} and {@link Mapper} to
   * the list of inputs for the map-reduce job. Returns the instance files
   * created.
   * 
   * @param job
   *          The {@link Job}
   * @param path
   *          {@link Path} to be added to the list of inputs for the job
   * @param inputFormat
   *          {@link InputFormat} class to use for this path
   * @param mapperInstance
   *          {@link Mapper} instance to use
   * @inputId An integer identifier that can be used to make every assignment
   *          unique. Otherwise the same input path can't be associated with
   *          more than one mapper (advanced usage).
   * @throws IOException
   * @throws FileNotFoundException
   */
  public static Set<String> addInputPath(Job job, Path path, InputFormat inputFormat, Mapper mapperInstance,
      Map<String, String> specificContext, int inputId) throws FileNotFoundException, IOException {

    Set<String> instanceFiles = new HashSet<String>();
    // Serialize the Mapper instance
    String uniqueNameMapper = UUID.randomUUID().toString() + '.' + "mapper.dat";
    try {
      InstancesDistributor.distribute(mapperInstance, uniqueNameMapper, job.getConfiguration());
      instanceFiles.add(uniqueNameMapper);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    // Serialize the Input Format
    String uniqueNameInputFormat = UUID.randomUUID().toString() + '.' + "inputFormat.dat";
    try {
      InstancesDistributor.distribute(inputFormat, uniqueNameInputFormat, job.getConfiguration());
      instanceFiles.add(uniqueNameInputFormat);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    for (Map.Entry<String, String> contextKeyValue : specificContext.entrySet()) {
      PangoolMultipleInputs.addInputContext(job, uniqueNameInputFormat, contextKeyValue.getKey(), contextKeyValue
          .getValue(), inputId);
    }
    addInputPath(job, path, uniqueNameInputFormat, inputId);
    Configuration conf = job.getConfiguration();
    conf.set(PANGOOL_INPUT_DIR_MAPPERS_PREFIX_CONF + path.toString() + "." + inputId, uniqueNameMapper);
    job.setMapperClass(DelegatingMapper.class);
    return instanceFiles;
  }

  private static void addInputPath(Job job, Path path, String inputFormatInstance, int inputId) {
    /*
     * Only internal -> not allowed to add inputs without associated
     * InputProcessor files
     */
    Configuration conf = job.getConfiguration();
    conf.set(PANGOOL_INPUT_DIR_FORMATS_PREFIX_CONF + path.toString() + "." + inputId, inputFormatInstance);

    job.setInputFormatClass(DelegatingInputFormat.class);
  }

  static Map<Path, List<String>> getInputFormatMap(JobContext job) {
    Map<Path, List<String>> m = new HashMap<Path, List<String>>();
    Configuration conf = job.getConfiguration();
    for (Map.Entry<String, String> confEntry : conf) {
      if (confEntry.getKey().startsWith(PANGOOL_INPUT_DIR_FORMATS_PREFIX_CONF)) {
        String key = confEntry.getKey().substring(PANGOOL_INPUT_DIR_FORMATS_PREFIX_CONF.length(),
            confEntry.getKey().length());
        // remove inputId
        key = key.substring(0, key.lastIndexOf("."));
        Path p = new Path(key);
        List<String> inputFormats = m.get(p);
        if (inputFormats == null) {
          inputFormats = new ArrayList<String>();
          m.put(p, inputFormats);
        }
        inputFormats.add(confEntry.getValue());
      }
    }
    return m;
  }

  /**
   * Retrieves a map of {@link Path}s to the serialized {@link TupleMapper} that
   * should be used for them.
   * 
   * @param job
   *          The {@link JobContext}
   * @return A map of paths to InputProcessor instances for the job
   */
  static Map<Path, List<String>> getInputProcessorFileMap(JobContext job) {
    Configuration conf = job.getConfiguration();
    Map<Path, List<String>> m = new HashMap<Path, List<String>>();
    for (Map.Entry<String, String> confEntry : conf) {
      if (confEntry.getKey().startsWith(PANGOOL_INPUT_DIR_MAPPERS_PREFIX_CONF)) {
        String key = confEntry.getKey().substring(PANGOOL_INPUT_DIR_MAPPERS_PREFIX_CONF.length(),
            confEntry.getKey().length());
        // remove inputId
        key = key.substring(0, key.lastIndexOf("."));
        Path p = new Path(key);
        List<String> mappers = m.get(p);
        if (mappers == null) {
          mappers = new ArrayList<String>();
          m.put(p, mappers);
        }
        mappers.add(confEntry.getValue());
      }
    }
    return m;
  }

  /**
   * Specific (key, value) configurations for each Input. Some Input Formats
   * read specific configuration values and act based on them.
   */
  public static void addInputContext(Job job, String inputName, String key, String value, int inputId) {
    // Check that this named output has been configured before
    Configuration conf = job.getConfiguration();
    // Add specific configuration
    conf.set(MI_PREFIX + inputName + "." + inputId + CONF + "." + key, value);
  }

  /**
   * Iterates over the Configuration and sets the specific context found for the
   * input in the Job instance. Package-access so it can be unit tested. The
   * specific context is configured in method this.
   * {@link #addInputContext(Job, String, String, String)}
   */
  public static void setSpecificInputContext(Configuration conf, String inputName, int inputId) {
    for (Map.Entry<String, String> entries : conf) {
      String confKey = entries.getKey();
      String confValue = entries.getValue();
      if (confKey.startsWith(MI_PREFIX + inputName + "." + inputId + CONF)) {
        // Specific context key, value found
        String contextKey = confKey.substring((MI_PREFIX + inputName + "." + inputId + CONF + ".").length(), confKey
            .length());
        conf.set(contextKey, confValue);
      }
    }
  }
}
