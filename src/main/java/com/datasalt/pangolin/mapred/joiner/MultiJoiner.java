package com.datasalt.pangolin.mapred.joiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasalt.pangolin.commons.HadoopUtils;

/**
 * The MultiJoiner is a tool for easing the task of creating Map/Reduce Jobs that must perform a join over an arbitrary
 * number of "channels". For an illustrative example, see {@link TestMultiJoiner}.
 * <p>
 * The channels are identified by the user, and the API applies secondary sort on them so that instances from each
 * channel are received in a deterministic order in the reducer implementation. There is also the possibility to add one
 * more level of secondary sorting. For this, we need to provide a consistent {@link MultiJoinPair} by calling
 * setMultiJoinPairClass() method. For an illustrative example, check {@link TestMultiJoinerSecondarySort}.
 * <p>
 * This API supports any kind of serialization in the key and the value by leveraging Hadoop's Serialization API.
 * <p>
 * There are two flavors for using this API:
 * <ul>
 * <li>Channeled mappers: see {@link MultiJoinChanneledMapper}. You can associate one Mapper implementation to (only)
 *   one channel for concision and code reusing. It supports specifying globs as input paths, which will be expanded
 *   underneath.</li>
 * <li>Multi-channel mappers: see {@link MultiJoinMultiChannelMapper}. You can specify input mappers and channels in a
 *   separated fashion and specify the channel number in the emit() methods of your mapper for emitting more than one
 *   channel from the same mapper.</li>
 * </ul>
 * <p>
 * 
 * @author epalace, pere
 * 
 */
@SuppressWarnings({ "rawtypes" })
public class MultiJoiner {

	final static Logger log = LoggerFactory.getLogger(MultiJoiner.class);

	public static final String MULTIJOINER_CHANNELS = "pisae.multijoiner.channels";
	public static final String MULTIJOINER_CLASSES = "pisae.multijoiner.classes";

	public static final String MULTIJOINER_KEY_IMPL = "pisae.multijoiner.key.impl";

	private Job job = null;
	private String name;
	private Configuration conf;
	private Class<? extends MultiJoinReducer> reducer;
	private Class outputKeyClass;
	private Class outputValueClass;
	private Class<? extends OutputFormat> outputFormat;
	private Path outputPath;

	/**
	 * Use this constructor to get a base Job for your Multi-Joiner. You can then call addInput() sequentially to add
	 * inputs in join order.
	 * 
	 * @param name
	 * @param conf
	 * @throws IOException
	 */
	public MultiJoiner(String name, Configuration conf) {
		this.name = name;
		this.conf = conf;
	}

	public Job getJob() throws IOException {
		if(job == null) {
			job = new Job(conf, name);

			HadoopUtils.deleteIfExists(FileSystem.get(conf), outputPath);
			job.setJarByClass(MultiJoiner.class);

			job.setReducerClass(reducer);
			job.setMapOutputValueClass(MultiJoinDatum.class);
			job.setMapOutputKeyClass(MultiJoinPair.class);
			job.setOutputFormatClass(outputFormat);
			job.setOutputKeyClass(outputKeyClass);
			job.setOutputValueClass(outputValueClass);

			job.setGroupingComparatorClass(MultiJoinPair.GroupComparator.class);
			job.setPartitionerClass(MultiJoinPair.GroupPartitioner.class);
			FileOutputFormat.setOutputPath(job, outputPath);

			setMultiJoinPairClass(MultiJoinPair.class);
		}

		return job;
	}

	public static List<String> readStringListFromConfig(Configuration conf, String what) {
		List<String> clazzes = new ArrayList<String>();
		int id = 0;
		String[] classes = conf.get(what).split(",");
		for(String className : classes) {
			clazzes.add(className);
			id++;
		}
		return clazzes;
	}

	private static void addInOrder(String property, String where, Configuration conf) {
		String value = conf.get(where);
		if(value == null || value.length() < 1) {
			value = property;
		} else {
			value += "," + property;
		}
		conf.set(where, value);
	}

	public void addConf(String key, String value) {
		job.getConfiguration().set(key, value);
	}

	public Configuration associatedConf() {
		return job.getConfiguration();
	}

	public MultiJoiner setMultiJoinPairClass(Class<? extends MultiJoinPair> keyImpl) throws IOException {
		getJob().getConfiguration().set(MULTIJOINER_KEY_IMPL, keyImpl.getName());
		getJob().setMapOutputKeyClass(keyImpl);
		return this;
	}

	/**
	 * Adds an input specification. This input won't be associated to a channel as it will be a
	 * {@link MultiJoinMultiChannelMapper}.
	 * 
	 * @param location
	 * @param inputFormat
	 * @param mapper
	 * @return
	 * @throws IOException
	 */
	public MultiJoiner addInput(Path location, Class<? extends InputFormat> inputFormat,
	    Class<? extends MultiJoinMultiChannelMapper> mapper) throws IOException {
		MultipleInputs.addInputPath(getJob(), location, inputFormat, mapper);
		return this;
	}

	/**
	 * Adds a channel specification. The user is free to use this channel in any mapper.
	 * 
	 * @param channel
	 * @param channelClass
	 * @return
	 * @throws IOException
	 */
	public MultiJoiner setChannelDatumClass(Integer channel, Class<? extends Object> channelClass) throws IOException {
		addInOrder(channel + "", MULTIJOINER_CHANNELS, getJob().getConfiguration());
		addInOrder(channelClass.getName(), MULTIJOINER_CLASSES, getJob().getConfiguration());
		return this;
	}

	/**
	 * Adds a CHANNELED input specification. A channeled input specification is a channel associated to a Mapper and a
	 * input file or glob. The user will implement a {@link MultiJoinChanneledMapper} which will be tied to a single
	 * channel.
	 * <p>
	 * The user must be consistent with the channel numbers it provides. For instance, in case that two or more different
	 * files must belong to the same channel.
	 * 
	 * @param channel
	 * @param location
	 * @param channelClass
	 * @param inputFormat
	 * @param mapper
	 * @return
	 * @throws IOException
	 */
	public MultiJoiner addChanneledInput(Integer channel, Path location, Class<? extends Object> channelClass,
	    Class<? extends InputFormat> inputFormat, Class<? extends MultiJoinChanneledMapper> mapper) throws IOException {
		/*
		 * Configure the MultiJoiner
		 */
		setChannelDatumClass(channel, channelClass);
		FileSystem fS = FileSystem.get(getJob().getConfiguration());
		if(location.toString().contains("*")) { // is a glob
			for(FileStatus fSt : fS.globStatus(location)) { // expands the glob
				addChanneledInputInner(channel, fSt.getPath(), channelClass, inputFormat, mapper);
			}
		} else if(fS.getFileStatus(location).isDir()) {
			for(FileStatus fSt : fS.listStatus(location)) { // expands the glob
				addChanneledInputInner(channel, fSt.getPath(), channelClass, inputFormat, mapper);
			}
		} else {
			addChanneledInputInner(channel, location, channelClass, inputFormat, mapper);
		}
		return this;
	}

	private void addChanneledInputInner(Integer channel, Path location, Class<? extends Object> channelClass,
	    Class<? extends InputFormat> inputFormat, Class<? extends MultiJoinChanneledMapper> mapper) throws IOException {

		FileSystem fS = location.getFileSystem(getJob().getConfiguration());
		if(!location.toString().startsWith("/")) {
			// relative path
			location = new Path(fS.getWorkingDirectory(), location);
		} else {
			// absolute path
			location = new Path(fS.getUri() + location.toString());
		}
		addInOrder(channel + "", MultiJoinChanneledMapper.MULTIJOINER_CHANNELED_CHANNELS, getJob().getConfiguration());
		addInOrder(location.toString(), MultiJoinChanneledMapper.MULTIJOINER_CHANNELED_FILES, getJob().getConfiguration());
		MultipleInputs.addInputPath(getJob(), location, inputFormat, mapper);
	}

	public MultiJoiner setReducer(Class<? extends MultiJoinReducer> reducer) {
		this.reducer = reducer;
		return this;
	}

	public MultiJoiner setOutputKeyClass(Class outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
		return this;
	}

	public MultiJoiner setOutputValueClass(Class outputValueClass) {
		this.outputValueClass = outputValueClass;
		return this;
	}

	public MultiJoiner setOutputFormat(Class<? extends OutputFormat> outputFormat) {
		this.outputFormat = outputFormat;
		return this;
	}

	public MultiJoiner setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
		return this;
	}
}
