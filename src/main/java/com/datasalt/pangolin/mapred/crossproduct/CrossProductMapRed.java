package com.datasalt.pangolin.mapred.crossproduct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputsPatched;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasalt.pangolin.mapred.crossproduct.io.CrossProductExtraKey;
import com.datasalt.pangolin.mapred.crossproduct.io.CrossProductPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinChanneledMapper;
import com.datasalt.pangolin.mapred.joiner.MultiJoinDatum;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinReducer;
import com.datasalt.pangolin.mapred.joiner.MultiJoiner;

/**
 * The Cross Product Map/Red allows us to perform the Cross Product of two datasets. For example, if we have set A: { a,
 * b } and set B: { 1, 2, 3 }, the output of this job's A x B cross product would be 6 pairs: { a1, a2, a3, b1, b2, b3
 * }.
 * <p>
 * The job's Map/Reduce nature allows nesting by performing multiple cross-products, depending on the key that we emit
 * in the {@link CrossProductMapper} which in turn uses the {@link MultiJoiner} API. If we don't use always the same
 * key, then we will end up performing as many cross-products as reduce groups we have in the end (one per reduce
 * group).
 * <p>
 * The job's output is of type {@link CrossProductPair}. The user is responsible for deserializing the resulting pair.
 * <p>
 * There is one fundamental thing about this Job is that it can't assure to scale if the <b>RIGHT</b> (second) dataset
 * doesn't fit in memory. In order to scale, an "Extra" output is emitted in case that the RIGHT (second) dataset
 * doesn't fit in memory for a certain key. What we do in this case is split the problem in n smaller problems to be
 * grouped by another reducer in another job - which is called {@link BigGroupsCrossProductJob}. Because most of the
 * groups will fit in memory, the second Map/Reduce job is assured to run with very few data and be very short in time.
 * <p>
 * The property for configuring the size of the RIGHT (second) dataset that is not allowed to fit in memory is specified
 * in SPLIT_DATASET_SIZE_CONF. Its default value is SPLIT_DATASET_SIZE_DEFAULT.
 * <p>
 * Because the job may have two final outputs, the user can join them by calling the method getBigGroupsOutputGlob().
 * Also, the user may control the two-job flow externally by invoking methods like isSecondJobNeeded() or directly
 * invoke memoryAwareRun() and let the API execute the two jobs in a sequentail chain if needed.
 * 
 * @author pere
 * 
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class CrossProductMapRed<K, V> {

	static Logger log = LoggerFactory.getLogger(CrossProductMapRed.class);

	public final static String EXTRA_OUTPUT = "EXTRA";

	public final static String SPLIT_DATASET_SIZE_CONF = "cross.product.split.size";
	public final static Integer SPLIT_DATASET_SIZE_DEFAULT = 100000;

	public final static int FIRST_CHANNEL_IN_REDUCER = 0;
	public final static int SECOND_CHANNEL_IN_REDUCER = 1;

	public static enum Counters {
		TOTALGROUPS, NOTFITTINGINMEMORY, EMITTED, EMITTEDEXTRAOUTPUT
	}

	/**
	 * Mapper to extend for emitting datasets to the {@link CrossProductReducer}
	 * 
	 * @author pere
	 * 
	 * @param <K>
	 * @param <V>
	 * @param <T>
	 */
	public static class CrossProductMapper<K, V, T> extends MultiJoinChanneledMapper<K, V, T> {

	}

	/**
	 * This Reducer is called automatically by the Cross Product API.
	 * 
	 * @author pere
	 * 
	 */
	public static class CrossProductReducer extends MultiJoinReducer<CrossProductPair, NullWritable> {

		MultipleOutputsPatched mOs;
		int splitSize;
		List<BytesWritable> inMemoryData = new ArrayList<BytesWritable>(splitSize);
		CrossProductPair toEmit = new CrossProductPair();
		CrossProductExtraKey extraKey = new CrossProductExtraKey();

		byte[] empty = new byte[0];

		protected void setup(Context context) throws IOException, InterruptedException {

			super.setup(context);
			mOs = new MultipleOutputsPatched(context);
			splitSize = context.getConfiguration().getInt(SPLIT_DATASET_SIZE_CONF, SPLIT_DATASET_SIZE_DEFAULT);
			log.info("Cross product split size is [" + splitSize + "]");
		};

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mOs.close();
		};

		protected void reduce(MultiJoinPair key, Iterable<MultiJoinDatum<?>> values, Context ctx) throws IOException,
		    InterruptedException {

			ctx.getCounter("stats", Counters.TOTALGROUPS + "").increment(1);
			inMemoryData.clear(); // TODO try to avoid this and reuse buffers
			int nGroups = 0;

			for(MultiJoinDatum<?> datum : values) {
				/*
				 * We will receive the second dataset first (assigned to the multijoiner's FIRST_CHANNEL). We want it to fit in
				 * memory.
				 */
				if(datum.getChannelId() == FIRST_CHANNEL_IN_REDUCER) {
					BytesWritable newWritable = new BytesWritable(); // TODO try to avoid this and reuse buffers
					newWritable.set(datum.getDatum());
					inMemoryData.add(newWritable);
					ctx.getCounter("stats", "CHANNEL_" + FIRST_CHANNEL_IN_REDUCER).increment(1);
					if(inMemoryData.size() == splitSize) {
						/*
						 * It doesn't fit in memory. The problem needs to be split: Emit to a different output
						 */
						if(nGroups == 0) {
							ctx.getCounter("stats", Counters.NOTFITTINGINMEMORY + "").increment(1);
						}
						for(BytesWritable data : inMemoryData) {
							/*
							 * The key for the second job's reducer will have "nGroups" nested in it.
							 */
							extraKey.setGroupId(nGroups);
							extraKey.setGroup(key.getMultiJoinGroup());
							toEmit.setLeft(empty);
							toEmit.setRight(data);
							mOs.write(EXTRA_OUTPUT, extraKey, toEmit);
							ctx.getCounter("stats", Counters.EMITTEDEXTRAOUTPUT + "").increment(1);
						}
						inMemoryData.clear();
						nGroups++;
					}
				} else {
					/*
					 * Here we are receiving the first dataset (assigned to the multijoiner's SECOND_CHANNEL). If we have put the
					 * second dataset all in memory, we will perform the cross-product on-the-fly.
					 */
					ctx.getCounter("stats", "CHANNEL_" + SECOND_CHANNEL_IN_REDUCER).increment(1);
					if(nGroups == 0) {
						/*
						 * The problem fits in memory
						 */
						if(inMemoryData.size() == 0) {
							ctx.getCounter("stats", "UNMATCHED_" + SECOND_CHANNEL_IN_REDUCER).increment(1);
						}
						for(BytesWritable data : inMemoryData) {
							toEmit.setLeft(datum.getDatum());
							toEmit.setRight(data);
							ctx.write(toEmit, NullWritable.get());
							ctx.getCounter("stats", Counters.EMITTED + "").increment(1);
						}
					} else {
						/*
						 * We already know the problem doesn't fit in memory
						 */
						if(inMemoryData.size() > 0) {
							/*
							 * We check this because we might have split size = 1000 but a list of let's say 680 pending to be
							 * flushed. Then, we need to emit pending in-memory data from second dataset
							 */
							for(BytesWritable data : inMemoryData) {
								extraKey.setGroupId(nGroups);
								extraKey.setGroup(key.getMultiJoinGroup());
								toEmit.setLeft(empty);
								toEmit.setRight(data);
								mOs.write(EXTRA_OUTPUT, extraKey, toEmit);
								ctx.getCounter("stats", Counters.EMITTEDEXTRAOUTPUT + "").increment(1);
							}
							inMemoryData.clear();
						}
						/*
						 * Emit first dataset to "EXTRA" output
						 */
						for(int i = 0; i < nGroups; i++) {
							/*
							 * We have to replicate the dataset as many times as sub-groups we have created for the next Job.
							 */
							extraKey.setGroupId(i);
							extraKey.setGroup(key.getMultiJoinGroup());
							toEmit.setLeft(datum.getDatum());
							toEmit.setRight(empty);
							mOs.write(EXTRA_OUTPUT, extraKey, toEmit);
							ctx.getCounter("stats", Counters.EMITTEDEXTRAOUTPUT + "").increment(1);
						}
					}
				}
			}
		}
	}

	private Job job = null;
	private String name;
	private Class<? extends OutputFormat> outputFormat;
	private Configuration conf;
	private Path leftInputPath, rightInputPath, outputPath;
	private Class<? extends InputFormat> leftInputFormat, rightInputFormat;
	private Class<? extends CrossProductMapper> leftInputMapper, rightInputMapper;

	public CrossProductMapRed(String name, Configuration conf) {
		this.name = name;
		this.conf = conf;
	}

	public void setLeftInputPath(Path leftInputPath) {
		this.leftInputPath = leftInputPath;
	}

	public void setLeftInputFormat(Class<? extends InputFormat> leftInputFormat) {
		this.leftInputFormat = leftInputFormat;
	}

	public void setRightInputPath(Path rightInputPath) {
		this.rightInputPath = rightInputPath;
	}

	public void setRightInputFormat(Class<? extends InputFormat> rightInputFormat) {
		this.rightInputFormat = rightInputFormat;
	}

	public void setOutputFormat(Class<? extends OutputFormat> outputFormat) {
		this.outputFormat = outputFormat;
	}

	public void setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
	}

	public void setLeftInputMapper(Class<? extends CrossProductMapper> leftInputMapper) {
		this.leftInputMapper = leftInputMapper;
	}

	public void setRightInputMapper(Class<? extends CrossProductMapper> rightInputMapper) {
		this.rightInputMapper = rightInputMapper;
	}

	public Job getJob() throws IOException {

		if(job == null) {
			MultiJoiner multiJoiner = new MultiJoiner(name, conf);
			multiJoiner.setReducer(CrossProductReducer.class);
			multiJoiner.setOutputKeyClass(CrossProductPair.class);
			multiJoiner.setOutputValueClass(NullWritable.class);
			multiJoiner.setOutputFormat(outputFormat);
			multiJoiner.setOutputPath(outputPath);

			Job job = multiJoiner
			    .addChanneledInput(SECOND_CHANNEL_IN_REDUCER, leftInputPath, Object.class, leftInputFormat, leftInputMapper)
			    .addChanneledInput(FIRST_CHANNEL_IN_REDUCER, rightInputPath, Object.class, rightInputFormat, rightInputMapper)
			    .getJob();

			/*
			 * Outputs
			 */
			MultipleOutputsPatched.setCountersEnabled(job, true);
			MultipleOutputsPatched.addNamedOutput(job, EXTRA_OUTPUT, SequenceFileOutputFormat.class,
			    CrossProductExtraKey.class, CrossProductPair.class);
			this.job = job;
		}
		return job;

	}

	/**
	 * By invoking this method, we leave the API the responsability of running 1 or 2 Jobs, depending on the first Job
	 * output.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void memoryAwareRun() throws IOException, InterruptedException, ClassNotFoundException {
		getJob().waitForCompletion(true);
		if(isSecondJobNeeded()) {
			getSecondJob().waitForCompletion(true);
		}
	}

	/**
	 * By invoking this method after the job has run, we can know if we need to run a second Job to perform the full
	 * cross-product. It is the user responsability to do something with this information.
	 * 
	 * @param job
	 * @return
	 * @throws IOException
	 */
	public boolean isSecondJobNeeded() throws IOException {
		return getJob().getCounters().getGroup("stats").findCounter(Counters.NOTFITTINGINMEMORY + "").getValue() > 0;
	}

	/**
	 * This Path points to the Glob that will be input to the second Map/Red Job. Useful for unit testing.
	 * 
	 * @return
	 */
	public Path getBigGroupsGlob() {
		return new Path(outputPath, CrossProductMapRed.EXTRA_OUTPUT + "*");
	}

	/**
	 * This Path points to the output of the second Map/Red Job, which is an extension to the output provided by the user
	 * in the first Job.
	 * 
	 * @return
	 */
	public Path getBigGroupsOutput() {
		return new Path(outputPath + "_big_groups");
	}

	/**
	 * This Path points to the Glob representing the useful output of the second Job.
	 * 
	 * @return
	 */
	public Path getBigGroupsOutputGlob() {
		return new Path(getBigGroupsOutput() + "/" + "part*");
	}

	/**
	 * This method returns the second Job that can be executed after the first Job if needed.
	 * 
	 * @return
	 * @throws IOException
	 */
	public Job getSecondJob() throws IOException {
		return BigGroupsCrossProductJob.get(getBigGroupsGlob(), outputPath.toString(), outputFormat, outputPath
		    + "_big_groups", conf);
	}
}