package com.datasalt.pangolin.mapred.joiner;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.mapred.PangolinMapper;

/**
 * This is the base abstract class that can be used to implement mappers for {@link MultiJoiner} jobs. It contains all
 * the base logic that uses {@link MultiJoinPair} for the key and {@link MultiJoinDatum} for the values. It uses the
 * hadoop serialization ({@link Serialization} to convert objects into byte[].
 * <p>
 * See {@link MultiJoinMultiChannelMapper} for a mapper that can emit more than one channel from the same mapper and
 * {@link MultiJoinChanneledMapper} for a mapper that can be configured to use always the same channel.
 * 
 * @author pere
 * 
 * @param <INPUT_KEY>
 * @param <INPUT_VALUE>
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class MultiJoinMapperBase<INPUT_KEY, INPUT_VALUE> extends PangolinMapper<INPUT_KEY, INPUT_VALUE, MultiJoinPair, MultiJoinDatum> {

	protected MultiJoinPair key;
	protected MultiJoinDatum datum = new MultiJoinDatum();
	protected Context context;

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.context = context;
		try {
			this.key = (MultiJoinPair) Class.forName(context.getConfiguration().get(MultiJoiner.MULTIJOINER_KEY_IMPL))
			    .newInstance();
		} catch(Exception e) {
			throw new IOException(e);
		}
	};

	protected void emitBytes(byte[] grouping, int offset, int length, WritableComparable secondarySort, Object datum,
	    int channel) throws IOException, InterruptedException {

		key.setMultiJoinGroup(grouping, offset, length);
		key.setChannelId(channel);
		if(secondarySort != null) {
			key.setSecondSort(secondarySort);
		}
		this.datum.setDatum(ser.ser(datum));
		this.datum.setChannelId(channel);
		context.write(key, this.datum);
	}
}
