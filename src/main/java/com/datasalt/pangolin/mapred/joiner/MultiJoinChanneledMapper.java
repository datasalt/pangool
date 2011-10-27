package com.datasalt.pangolin.mapred.joiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.GetInputFileFromTaggedInputSplit;

/**
 * This mapper is associated with one channel and emits always the same kind of data. This enables concise code and code
 * reusing in some cases. It must be configured with the appropriate methods in {@link MultiJoiner} that accept such
 * mapper classes.
 * 
 * @author pere
 * 
 * @param <INPUT_KEY>
 *          Any type for the input key
 * @param <INPUT_VALUE>
 *          Any type for the input value
 * @param <OUTPUT_VALUE>
 *          A type that will be serialized through the channel
 */
@SuppressWarnings({ "rawtypes" })
public class MultiJoinChanneledMapper<INPUT_KEY, INPUT_VALUE, OUTPUT_VALUE> extends MultiJoinMapperBase<INPUT_KEY, INPUT_VALUE> {

	public static final String MULTIJOINER_CHANNELED_FILES = "pisae.multijoiner.channeled.files";
	public static final String MULTIJOINER_CHANNELED_CHANNELS = "pisae.multijoiner.channeled.channels";

	private Map<String, Integer> idByFile = new HashMap<String, Integer>();

	/**
	 * Don't forget to call super.setup() if you override this method.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		int id = 0;
		List<String> channels = MultiJoiner.readStringListFromConfig(context.getConfiguration(),
		    MULTIJOINER_CHANNELED_CHANNELS);
		Set<String> distinctChannels = new HashSet<String>();
		for(String file : MultiJoiner.readStringListFromConfig(context.getConfiguration(), MULTIJOINER_CHANNELED_FILES)) {
			String thisChannel = channels.get(id);
			distinctChannels.add(thisChannel);
			int channel = Integer.parseInt(thisChannel);
			idByFile.put(file, channel);
			id++;
		}
	}

	/*
	 * The following methods can be used as a shortcut for emit(Object, SS, T)
	 */
	protected void emit(String grouping, OUTPUT_VALUE datum) throws IOException, InterruptedException {
		byte[] array = grouping.getBytes("UTF-8");
		emitBytes(array, 0, array.length, null, datum);
	}
	
	protected void emit(String grouping, WritableComparable secondarySort, OUTPUT_VALUE datum) throws IOException, InterruptedException {
		byte[] array = grouping.getBytes("UTF-8");
		emitBytes(array, 0, array.length, secondarySort, datum);
	}


	protected void emit(Object grouping, OUTPUT_VALUE datum) throws IOException, InterruptedException {
		byte[] array = ser.ser(grouping);
		emitBytes(array, 0, array.length, null, datum);
	}

	protected void emit(Text grouping, WritableComparable secondarySort, OUTPUT_VALUE datum) throws IOException,
	    InterruptedException {
		emitBytes(grouping.getBytes(), 0, grouping.getLength(), secondarySort, datum);
	}

	protected void emit(Text grouping, OUTPUT_VALUE datum) throws IOException, InterruptedException {
		emitBytes(grouping.getBytes(), 0, grouping.getLength(), null, datum);
	}

	/**
	 * You can use whichever WritableComparable as secondary sort, but you have to do two things:<br>
	 * 1) Create a {@link MultiJoinPair} for your WritableComparable. Look for example {@link MultiJoinPairText}. 2) Call
	 * the method {@link MultiJoiner#setMultiJoinPairClass(Class)} to set the proper {@link MultiJoinPair}
	 */
	protected void emit(Object grouping, WritableComparable secondarySort, OUTPUT_VALUE datum) throws IOException,
	    InterruptedException {
		byte[] array = ser.ser(grouping);
		emitBytes(array, 0, array.length, secondarySort, datum);
	}

	/**
	 * Emits any object as key, any object as value and optionally a WritableComparator for secondary sorting. The
	 * WritableComparator used here must be consistent with the {@link MultiJoinPair} class defined in the MultiJoiner
	 * configuration.
	 * 
	 * @param <SS>
	 * @param grouping
	 * @param secondarySort
	 * @param datum
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void emitBytes(byte[] grouping, int offset, int length, WritableComparable secondarySort, OUTPUT_VALUE datum)
	    throws IOException, InterruptedException {

		String path = GetInputFileFromTaggedInputSplit.get(context.getInputSplit());
		int classId = idByFile.get(path);
		emitBytes(grouping, offset, length, secondarySort, datum, classId);
	}
}