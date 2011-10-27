package com.datasalt.pangolin.mapred.joiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.mapred.PangolinReducer;

/**
 * You can extend MultiJoinReducer in order to have a shorter implementation of your {@link MultiJoiner} reducer. You can use the
 * deserialize() method deserializing the value and deserializeKey() for deserializing the key.
 * <p>
 * K is the output key type and V is the output value type.
 */
@SuppressWarnings("rawtypes")
public abstract class MultiJoinReducer<KEYOUT, VALUEOUT> extends PangolinReducer<MultiJoinPair, MultiJoinDatum<?>, KEYOUT, VALUEOUT> {

	final static Logger log = LoggerFactory.getLogger(MultiJoinReducer.class);
	private HashMap<Integer, Object> instances = new HashMap<Integer, Object>();
	private HashMap<Integer, Class> classes = new HashMap<Integer, Class>();
	private Configuration conf;
	
	private Serialization serialization;

	@SuppressWarnings("unchecked")
  @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			Configuration conf = context.getConfiguration();
			this.conf = conf;
			serialization = new Serialization(conf);
			/*
			 * Creating a cached instance of each class for each channel
			 */
			String []classes = conf.getStrings(MultiJoiner.MULTIJOINER_CLASSES);
			String []channels = conf.getStrings(MultiJoiner.MULTIJOINER_CHANNELS);
			for (int i=0; i<classes.length; i++) {
				Class c = Class.forName(classes[i]);				
        Object instance = ReflectionUtils.newInstance(c, conf);
				instances.put(new Integer(channels[i]), instance);
				this.classes.put(new Integer(channels[i]), c);
			}
			
		} catch(Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * Use this method to deserialize the value that you emitted in the MultiJoin Mappers.
	 * The returned object could be reused to successive calls to deserialize. Use
	 * {@link #deserializeNewInstance(MultiJoinDatum)} for non cached version
	 */
	public <T> T deserialize(MultiJoinDatum datum) throws IOException {
		Object obj = instances.get(datum.getChannelId());
		return serialization.<T> deser(obj, datum.getDatum());
	}
	
	/**
	 * Use this method to deserialize the value that you emitted in the MultiJoin Mappers.
	 * The returned object is always a new instance. Use {@link #deserialize(MultiJoinDatum)}
	 * for a cached version.
	 */
	@SuppressWarnings("unchecked")
  public <T> T deserializeNewInstance(MultiJoinDatum datum) throws IOException {
		Object obj = ReflectionUtils.newInstance(classes.get(datum.getChannelId()), conf);
		return serialization.<T> deser(obj, datum.getDatum());
	}


	/**
	 * Use this method to deserialize the key that you emitted in the MultiJoin Mappers.
	 */
	public <T> T deserializeKey(MultiJoinPair pair, T obj) throws IOException {
		return serialization.<T> deser(obj, pair.getMultiJoinGroup());
	}

	/*
	 * This method can be used as shortcut for deserializing from Text keys
	 */

	public Text deserializeKey(MultiJoinPair pair, Text obj) throws IOException {
		obj.set(pair.getMultiJoinGroup().getBytes(), 0, pair.getMultiJoinGroup().getLength());
		return obj;
	}

	/**
	 * Deprecated
	 * I think that deserializing to Text is faster (epalace)
	 *	
	 */
	@Deprecated
	public String deserializeKeyToString(MultiJoinPair pair) throws IOException {
		return new String(pair.getMultiJoinGroup().getBytes(), 0, pair.getMultiJoinGroup().getLength());
	}
}