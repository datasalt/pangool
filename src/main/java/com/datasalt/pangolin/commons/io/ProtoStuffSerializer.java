package com.datasalt.pangolin.commons.io;

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
