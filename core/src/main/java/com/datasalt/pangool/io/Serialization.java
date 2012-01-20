/**
 * Copyright [2011] [Datasalt Systems S.L.]
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

package com.datasalt.pangool.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * You can use this utility class to serialize / deserialize anything in the Hadoop context.
 * It is thread safe. Instantiate once, reuse many times. Otherwise it is not
 * efficient.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class Serialization {

	private SerializationFactory serialization;
	
	public Serialization(Configuration conf) throws IOException {
		serialization = new SerializationFactory(conf);
	}
	
	private ThreadLocal<DataInputBuffer> cachedInputStream = new ThreadLocal<DataInputBuffer>() {

		@Override
    protected DataInputBuffer initialValue() {
			return new DataInputBuffer();
    }
	};

	/**
	 * Serializes the given object using the Hadoop 
	 * serialization system.
	 */
	public void ser(Object datum,OutputStream output) throws IOException {
    Serializer ser = serialization.getSerializer(datum.getClass());
		ser.open(output);
		ser.serialize(datum);
		ser.close();
	}

	/**
	 * Deseerializes into the given object using the Hadoop 
	 * serialization system. Object cannot be null.
	 */
	public <T> T deser(Object obj,InputStream in) throws IOException {
		Deserializer deSer = serialization.getDeserializer(obj.getClass());
		deSer.open(in);
		obj = deSer.deserialize(obj);
		deSer.close();
		return (T)obj;
	}
	
	/** 
	 * Return a new instance of the given class with the 
	 * deserialized data from the input stream. 
	 */
	public <T> T deser(Class clazz,InputStream in) throws IOException {
		Deserializer deSer = serialization.getDeserializer(clazz);
		deSer.open(in);
		Object obj = deSer.deserialize(null);
		deSer.close();
		return (T)obj;
	}
		
	/**
	 * Deserialize an object using Hadoop serialization from a byte array. 
	 * The object cannot be null. 
	 */
	public <T> T deser(Object obj, byte[] array, int offset, int length) throws IOException {
	  Deserializer deSer = serialization.getDeserializer(obj.getClass());
		DataInputBuffer baIs = cachedInputStream.get();
		baIs.reset(array, offset,length);
		deSer.open(baIs);
		obj = deSer.deserialize(obj);
		deSer.close();
		baIs.close();
    return (T)obj;
	}
}
