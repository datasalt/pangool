package com.datasalt.pangolin.mapred.crossproduct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.datasalt.pangolin.mapred.crossproduct.io.CrossProductExtraKey;
import com.datasalt.pangolin.mapred.crossproduct.io.CrossProductPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinChanneledMapper;
import com.datasalt.pangolin.mapred.joiner.MultiJoinDatum;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPair;
import com.datasalt.pangolin.mapred.joiner.MultiJoinPairText;
import com.datasalt.pangolin.mapred.joiner.MultiJoinReducer;
import com.datasalt.pangolin.mapred.joiner.MultiJoiner;

/**
 * This Job is an optional step that is applied depending on the output of {@link CrossProductMapRed}. See the Cross
 * Product API for reference.
 * <p>
 * This Job shouldn't be instantiated or used directly.
 * 
 * @author pere
 */
@SuppressWarnings({ "rawtypes" })
public class BigGroupsCrossProductJob {

	public static enum Counters {
		TOTALGROUPS, ROWSEMITTED
	}

	public final static byte A = 'A';
	public final static byte B = 'B';

	public static class BigGroupsMap extends
	    MultiJoinChanneledMapper<CrossProductExtraKey, CrossProductPair, CrossProductPair> {

		Text secondarySort = new Text();

		@Override
		protected void map(CrossProductExtraKey key, CrossProductPair value, Context context) throws IOException,
		    InterruptedException {

			if(value.getRight().getLength() > 0) {
				secondarySort.set(A + "");
			} else {
				secondarySort.set(B + "");
			}
			emit(key, secondarySort, value);
		};
	}

	public static class BigGroupsRed extends MultiJoinReducer<CrossProductPair, NullWritable> {

		List<BytesWritable> memoryList = new ArrayList<BytesWritable>();

		protected void reduce(MultiJoinPair key, Iterable<MultiJoinDatum<?>> values, Context ctx) throws IOException,
		    InterruptedException {

			memoryList.clear();
			ctx.getCounter("stats", Counters.TOTALGROUPS + "").increment(1);

			for(MultiJoinDatum bag : values) {
				/*
				 *
				 *
				 */
				CrossProductPair pair = deserialize(bag);

				if(pair.getRight().getLength() > 0) {
					BytesWritable newWritable = new BytesWritable();
					newWritable.set(pair.getRight());
					memoryList.add(newWritable); // TODO maybe not really necessary to copy array
				} else {
					/*
					 * Perform cross-product
					 */
					for(BytesWritable element : memoryList) {
						pair.setRight(element);

						ctx.write(pair, NullWritable.get());
						ctx.getCounter("stats", Counters.ROWSEMITTED + "").increment(1);
					}
				}
			}
		};
	}

	public static Job get(Path inputGlob, String basePath, Class<? extends OutputFormat> outputFormat, String outPath,
	    Configuration conf) throws IOException {

		MultiJoiner multiJoiner = new MultiJoiner("Big Groups Cross Product", conf);
		multiJoiner.setReducer(BigGroupsRed.class);
		multiJoiner.setOutputKeyClass(CrossProductPair.class);
		multiJoiner.setOutputValueClass(NullWritable.class);
		multiJoiner.setOutputFormat(outputFormat);
		multiJoiner.setOutputPath(new Path(outPath));

		return multiJoiner
		    .addChanneledInput(0, inputGlob, CrossProductPair.class, SequenceFileInputFormat.class, BigGroupsMap.class)
		    .setMultiJoinPairClass(MultiJoinPairText.class).getJob();
	}
}
