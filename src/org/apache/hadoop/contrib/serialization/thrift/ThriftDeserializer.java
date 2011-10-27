/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.contrib.serialization.thrift;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

@SuppressWarnings("rawtypes")
class ThriftDeserializer<T extends TBase> implements Deserializer<T> {

  private Class<T> tClass;
  private TIOStreamTransport transport;
  private TProtocol protocol;
  
  public ThriftDeserializer(Class<T> tBaseClass) {
    this.tClass = tBaseClass;
  }

  public void open(InputStream in) {
    transport = new TIOStreamTransport(in);
    protocol = new TBinaryProtocol(transport);
  }
  
  public T deserialize(T t) throws IOException {
    T object = (t == null ? newInstance() : t);
    try {
    	// IVAN: Clearing the object!!! Otherwise some not incoming fields could remain.  
    	object.clear();
      object.read(protocol);
    } catch (TException e) {
      throw new IOException(e.toString());
    }
    return object;
  }
  
  private T newInstance() {
    return (T) ReflectionUtils.newInstance(tClass, null);
  }

  public void close() throws IOException {
    if (transport != null) {
      transport.close();
    }
  }

}
