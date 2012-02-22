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
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProtoStuffSerializer<T extends Schema> implements Serializer<T> {
		
	OutputStream out;
	static ThreadLocal<LinkedBuffer> threadLocalBuffer = new ThreadLocal<LinkedBuffer>() {

		LinkedBuffer buffer = LinkedBuffer.allocate(512);

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
