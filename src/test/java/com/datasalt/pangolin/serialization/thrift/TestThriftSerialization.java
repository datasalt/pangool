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
package com.datasalt.pangolin.serialization.thrift;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.record.Buffer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import sun.security.action.GetLongAction;

import com.datasalt.pangolin.commons.io.ProtoStuffSerialization;
import com.datasalt.pangolin.serialization.thrift.test.ObjForTest;

/**
 * Test the class {@link ThriftSerialization}
 */
public class TestThriftSerialization {
	
	SerializationFactory serFactory;
	
	@Before
	public void startup() {
		Configuration conf = new Configuration();
		ThriftSerialization.enableThriftSerialization(conf);
		serFactory = new SerializationFactory(conf);
	}
	
	@Test
	public void testSerDeser() throws IOException {
		
		DataOutputBuffer out = new DataOutputBuffer();
		ObjForTest obj = new ObjForTest();
		obj.setId("A");
		
		Serializer<ObjForTest> ser = serFactory.getSerializer(ObjForTest.class);
		Deserializer<ObjForTest> deser = serFactory.getDeserializer(ObjForTest.class);
		
		ser.open(out);
		ser.serialize(obj);
		ser.close();
		
		// Testing object creation
		DataInputBuffer in = new DataInputBuffer();
		in.reset(out.getData(), 0, out.getLength());
		
		deser.open(in);
		ObjForTest obj2 = deser.deserialize(null);
		deser.close();
		
		Assert.assertEquals(obj, obj2);
		
		// Testing object clearing and reusing
		in.reset(out.getData(), 0, out.getLength());
		ObjForTest reuseObj = new ObjForTest("A", "B");
		
		deser.open(in);
		obj2 = deser.deserialize(reuseObj);
		deser.close();

		Assert.assertTrue(reuseObj == obj2);
		Assert.assertEquals(obj, obj2);
	}

	@Test
	public void testPararellism() throws IOException {
		DataOutputBuffer out1 = new DataOutputBuffer();
		DataOutputBuffer out2 = new DataOutputBuffer();
		
		ObjForTest obj = new ObjForTest();
		obj.setId("A");
		
		Serializer<ObjForTest> ser1 = serFactory.getSerializer(ObjForTest.class);
		Serializer<ObjForTest> ser2 = serFactory.getSerializer(ObjForTest.class);

		Deserializer<ObjForTest> deser1 = serFactory.getDeserializer(ObjForTest.class);
		Deserializer<ObjForTest> deser2 = serFactory.getDeserializer(ObjForTest.class);
		
		// Asserting that the serialization is thread safe		
		ser1.open(out1);
		ser2.open(out2);
		ser1.serialize(obj);
		ser2.serialize(obj);
		ser1.close();
		ser2.close();
		
		Assert.assertTrue(Arrays.equals(out1.getData(), out2.getData()));

		// Asserting that the deserialization is thread safe
		ObjForTest obj1, obj2, obj3;
		DataInputBuffer in1 = new DataInputBuffer();
		in1.reset(out1.getData(), 0, out1.getLength());
		DataInputBuffer in2 = new DataInputBuffer();
		in2.reset(out2.getData(), 0, out2.getLength());
		
		deser1.open(in1);
		deser2.open(in2);
		obj1 = deser1.deserialize(null);
		obj2 = deser2.deserialize(null);
		deser1.close();
		deser2.close();
		
		Assert.assertEquals(obj1, obj2);
		Assert.assertEquals(obj, obj1);
	}
}
