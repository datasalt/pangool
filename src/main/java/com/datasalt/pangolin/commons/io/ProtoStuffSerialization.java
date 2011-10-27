package com.datasalt.pangolin.commons.io;

import com.datasalt.pangolin.commons.io.ProtoStuffDeserializer;
import com.datasalt.pangolin.commons.io.ProtoStuffSerializer;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import com.dyuproject.protostuff.Schema;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProtoStuffSerialization implements Serialization<Schema> {

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
}
