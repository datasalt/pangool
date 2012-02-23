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
package com.datasalt.pangool.mapreduce.lib.input;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.cogroup.processors.TupleMapper;
import com.datasalt.pangool.utils.DCUtils;

/**
 * This class supports MapReduce jobs that have multiple input paths with a different {@link InputFormat} and
 * {@link Mapper} for each path.
 * <p>
 * This class is inspired by the {@link org.apache.hadoop.lib.input.MultipleInputs} 
 */
@SuppressWarnings("rawtypes")
public class PangoolMultipleInputs {

	public final static String PANGOOL_INPUT_DIR_FORMATS_CONF = "pangool.input.dir.formats";
	public final static String PANGOOL_INPUT_DIR_MAPPERS_CONF = "pangool.input.dir.mappers";
	
	/**
	 * Add a {@link Path} with a custom {@link InputFormat} and {@link Mapper} to the list of inputs for the map-reduce
	 * job.
	 * 
	 * @param job
	 *          The {@link Job}
	 * @param path
	 *          {@link Path} to be added to the list of inputs for the job
	 * @param inputFormatClass
	 *          {@link InputFormat} class to use for this path
	 * @param mapperInstance
	 *          {@link Mapper} instance to use
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void addInputPath(Job job, Path path, Class<? extends InputFormat> inputFormatClass,
	     Mapper mapperInstance) throws FileNotFoundException, IOException {

		// Serialize the Mapper instance
		String uniqueName = UUID.randomUUID().toString() + '.' + "mapper.dat";
		try {
			DCUtils.serializeToDC(mapperInstance, uniqueName, job.getConfiguration());
		} catch(URISyntaxException e) {
			throw new IOException(e);
		}
		addInputPath(job, path, inputFormatClass);
		Configuration conf = job.getConfiguration();
		String mapperMapping = path.toString() + ";" + uniqueName;
		String mappers = conf.get(PANGOOL_INPUT_DIR_MAPPERS_CONF);
		conf.set(PANGOOL_INPUT_DIR_MAPPERS_CONF, mappers == null ? mapperMapping : mappers + "," + mapperMapping);
		job.setMapperClass(DelegatingMapper.class);
	}

	private static void addInputPath(Job job, Path path, Class<? extends InputFormat> inputFormatClass) {
		/*
		 * Only internal -> not allowed to add inputs without associated InputProcessor files
		 */
		String inputFormatMapping = path.toString() + ";" + inputFormatClass.getName();
		Configuration conf = job.getConfiguration();
		String inputFormats = conf.get(PANGOOL_INPUT_DIR_FORMATS_CONF);
		conf.set(PANGOOL_INPUT_DIR_FORMATS_CONF, inputFormats == null ? inputFormatMapping : inputFormats + ","
		    + inputFormatMapping);

		job.setInputFormatClass(DelegatingInputFormat.class);
	}

	/**
	 * Retrieves a map of {@link Path}s to the {@link InputFormat} class that should be used for them.
	 * 
	 * @param job
	 *          The {@link JobContext}
	 * @see #addInputPath(JobConf, Path, Class)
	 * @return A map of paths to inputformats for the job
	 */
	static Map<Path, InputFormat> getInputFormatMap(JobContext job) {
		Map<Path, InputFormat> m = new HashMap<Path, InputFormat>();
		Configuration conf = job.getConfiguration();
		String[] pathMappings = conf.get(PANGOOL_INPUT_DIR_FORMATS_CONF).split(",");
		for(String pathMapping : pathMappings) {
			String[] split = pathMapping.split(";");
			InputFormat inputFormat;
			try {
				inputFormat = (InputFormat) ReflectionUtils.newInstance(conf.getClassByName(split[1]), conf);
			} catch(ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			m.put(new Path(split[0]), inputFormat);
		}
		return m;
	}

	/**
	 * Retrieves a map of {@link Path}s to the serialized {@link TupleMapper} that should be used for them.
	 * 
	 * @param job
	 *          The {@link JobContext}
	 * @return A map of paths to InputProcessor instances for the job
	 */
	static Map<Path, String> getInputProcessorFileMap(JobContext job) {
		Configuration conf = job.getConfiguration();
		if(conf.get(PANGOOL_INPUT_DIR_MAPPERS_CONF) == null) {
			return Collections.emptyMap();
		}
		Map<Path, String> m = new HashMap<Path, String>();
		String[] pathMappings = conf.get(PANGOOL_INPUT_DIR_MAPPERS_CONF).split(",");
		for(String pathMapping : pathMappings) {
			String[] split = pathMapping.split(";");
			String inputProcessorFile = split[1];
			m.put(new Path(split[0]), inputProcessorFile);
		}
		return m;
	}
}
