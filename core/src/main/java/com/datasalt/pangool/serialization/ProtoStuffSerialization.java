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
package com.datasalt.pangool.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProtoStuffSerialization implements Serialization<Schema> {

	public static class ProtoStuffDeserializer<T extends Schema> implements Deserializer<T> {
	  
		private InputStream in;
	  private Class<T> tClass;
	  
	  public ProtoStuffDeserializer(Class<T> tClass) {
	  	this.tClass = tClass;
	  }
	  
		private static ThreadLocal<LinkedBuffer> threadLocalBuffer = new ThreadLocal<LinkedBuffer>() {

			private LinkedBuffer buffer = LinkedBuffer.allocate(512);

			@Override
			public LinkedBuffer get() {
				return buffer;
			}
		};
		
		@Override
	  public void open(InputStream in) throws IOException {
			this.in = in;
		}

	  @Override
	  public T deserialize(T t) throws IOException {
	  	t = (t == null) ? newInstance() : t;
			LinkedBuffer buff = threadLocalBuffer.get();
			ProtobufIOUtil.mergeDelimitedFrom(in, t, t, buff);
			buff.clear();
			return t;
	  }

	  private T newInstance() {
	    return (T) ReflectionUtils.newInstance(tClass, null);
	  }
	  
		@Override
	  public void close() throws IOException {

		}
	}
	
	public static class ProtoStuffSerializer<T extends Schema> implements Serializer<T> {
			
		private OutputStream out;
		protected static ThreadLocal<LinkedBuffer> threadLocalBuffer = new ThreadLocal<LinkedBuffer>() {

		protected	LinkedBuffer buffer = LinkedBuffer.allocate(512);

			@Override
			public LinkedBuffer get() {
				return buffer;
			}
		};
		
		@Override
	  public void open(OutputStream out) throws IOException {
			this.out = out;
	  }

	  @Override
	  public void serialize(T t) throws IOException {
			LinkedBuffer buff = threadLocalBuffer.get();
	  	ProtobufIOUtil.writeDelimitedTo(out, t, t, buff);
			buff.clear();
	  }

		@Override
	  public void close() throws IOException {

		}
	}
	
	
	@Override
  public boolean accept(Class<?> c) {
	  return Schema.class.isAssignableFrom(c);
  }

	@Override
  public Serializer<Schema> getSerializer(Class<Schema> c) {
	  return new ProtoStuffSerializer();
  }

	@Override
  public Deserializer<Schema> getDeserializer(Class<Schema> c) {
	  return new ProtoStuffDeserializer(c);
  }
	
  /**
   * Enables ProtoStuff Serialization support in Hadoop. 
   */
  public static void enableProtoStuffSerialization(Configuration conf) {
		String ser = conf.get("io.serializations").trim();
		if (ser.length() !=0 ) {
			ser += ",";
		}
		//Adding the ProtoStuff serialization
		ser += ProtoStuffSerialization.class.getName();
		conf.set("io.serializations", ser);
  }
}
