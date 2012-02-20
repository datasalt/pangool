package com.datasalt.pangool.io.tuple.ser;

import static org.apache.hadoop.io.WritableComparator.readDouble;
import static org.apache.hadoop.io.WritableComparator.readFloat;
import static org.apache.hadoop.io.WritableComparator.readInt;
import static org.apache.hadoop.io.WritableComparator.readLong;
import static org.apache.hadoop.io.WritableComparator.readVInt;
import static org.apache.hadoop.io.WritableComparator.readVLong;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.io.HadoopSerialization;

/**
 * A class for deserialize fields in Pangool format from a byte array.
 */
public class SingleFieldDeserializer {

	private final Configuration conf;
	private final CoGrouperConfig grouperConfig;
	private final HadoopSerialization ser;
	private final Map<Class<?>, Enum<?>[]> cachedEnums;
	
	public SingleFieldDeserializer(Configuration conf, CoGrouperConfig grouperConfig) throws IOException {
		this.conf = conf;
		this.grouperConfig = grouperConfig;
		this.ser = new HadoopSerialization(conf);
		this.cachedEnums = PangoolSerialization.getEnums(grouperConfig);
	} 
	
	/**
	 * Deserialize an individual field from a byte array position that is encoded with the 
	 * {@link PangoolSerialization}.
	 * <br>
	 * If the type is String, then {@link Text} is returned.

	 * @param instance An instance to be reused in the case of objects following standard 
	 * 				Hadoop serialization or String. If null, always return a new object. Usually
	 *        you should call this method first with a new instance, and then reuse this
	 *        instance in sucessive calls.  
	 * @param b1 The byte array.
	 * @param offset1 The place to start reading.
	 * @param type The type of the object to read.
	 * @return A deserialized instance.

	 */
	public Object deserialize(Object instance, byte[] bytes, int offset, Class<?> type) throws IOException {
		return deserialize(instance, bytes, offset, type, ser, conf, cachedEnums);
	}
	
	/**
	 * Deserialize an individual field from a byte array position that is encoded with the 
	 * {@link PangoolSerialization}.
	 * <br>
	 * If the type is String, then {@link Text} is returned.
	 * <br> 
	 * @param instance An instance to be reused in the case of objects following standard 
	 * 				Hadoop serialization or String. If null, always return a new object. Usually
	 *        you should call this method first with a new instance, and then reuse this
	 *        instance in sucessive calls.  
	 * @param b1 The byte array.
	 * @param offset1 The place to start reading.
	 * @param type The type of the object to read.
	 * @param ser A Hadoop serialization service for the cases of objects serialized 
	 *  			with this format
	 * @param conf A Hadoop configuration
	 * @param cachedEnums A map with the cached enumerations, for fast lookup. 
	 * @return A deserialized instance. 
	 */
	public static Object deserialize(Object instance, byte[] b1, int offset1, Class<?> type, 
  		HadoopSerialization ser, Configuration conf, Map<Class<?>, Enum<?>[]> cachedEnums) throws IOException {
  	if(type == Integer.class) {
  		// Integer
  		return readInt(b1, offset1);
  	} else if (type == String.class) {
  		if (instance == null) {
  			instance = ReflectionUtils.newInstance(Text.class, conf);				
  		}
  		int length = readVInt(b1, offset1);
  		return ser.deser(instance, b1, offset1, length);
  	} else if(type == Long.class) {
  		// Long
  		return readLong(b1, offset1);			
  	} else if(type == VIntWritable.class || type.isEnum()) {
  		// VInt || Enum
  		int value1 = readVInt(b1, offset1);
  		return (type.isEnum()) ? cachedEnums.get(type)[value1] : value1;			
  	} else if(type == VLongWritable.class) {
  		// VLong
  		return readVLong(b1, offset1);
  	} else if(type == Float.class) {
  		// Float
  		return readFloat(b1, offset1);
  	} else if(type == Double.class) {
  		// Double
  		return readDouble(b1, offset1);
  	} else if(type == Boolean.class) {
  		// Boolean
  		return b1[offset1] > 0;
  	} else {
  		if (instance == null) {
  			instance = ReflectionUtils.newInstance(type, conf);				
  		}
  		// Custom objects uses Hadoop Serialization, but has a header with the length.
  		int length = readVInt(b1, offset1);
  		offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
  		
  		return ser.deser(instance, b1, offset1, length);
  	} 
  }
	
	
}
