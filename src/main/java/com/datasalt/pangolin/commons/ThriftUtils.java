package com.datasalt.pangolin.commons;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

@SuppressWarnings("rawtypes")
public class ThriftUtils {

	private static TSerializer simpleJSONserializer = new TSerializer(new TSimpleJSONProtocol.Factory());
	
	private static ThreadLocal<TSerializer> serializer = new ThreadLocal<TSerializer>() {
		@Override
    protected TSerializer initialValue() {
	    return new TSerializer();
    }		
	};
	
	private static ThreadLocal<TDeserializer> deserializer = new ThreadLocal<TDeserializer>() {
		@Override
    protected TDeserializer initialValue() {
	    return new TDeserializer();
    }		
	};
	
	/**
	 * Serializes a Thrift object with the protocol 
	 * {@link TSimpleJSONProtocol}. This format cannot be
	 * deserialized. Useful for logging, etc.
	 * 
	 * Exceptions are ignored and returns an String with 
	 * the exception error message.
	 */
	public static String toJSON(TBase thriftObject) {
		try {
	    return simpleJSONserializer.toString(thriftObject);	    
    } catch (TException e) {
    	return e.getMessage();
    }
	}
	
	/**
	 * Return a Thrift serializer. One serializer is 
	 * cached per each thread (so it is thread safe)
	 */
	public static TSerializer getSerializer() {
		return serializer.get();
	}

	/**
	 * Return a Thrift deserializer. One deserializer is 
	 * cached per each thread (so it is thread safe)
	 */
	public static TDeserializer getDeserializer() {
		return deserializer.get();
	}
	
}
