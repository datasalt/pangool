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
package com.datasalt.pangool.serialization.protostuff;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProtoStuffDeserializer<T extends Schema> implements Deserializer<T> {
  
	InputStream in;
  private Class<T> tClass;
  
  public ProtoStuffDeserializer(Class<T> tClass) {
  	this.tClass = tClass;
  }
  
	private static ThreadLocal<LinkedBuffer> threadLocalBuffer = new ThreadLocal<LinkedBuffer>() {

		LinkedBuffer buffer = LinkedBuffer.allocate(512);

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
