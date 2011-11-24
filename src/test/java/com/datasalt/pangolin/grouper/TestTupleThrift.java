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


package com.datasalt.pangolin.grouper;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.FieldsDescription.Field;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.thrift.test.A;


/**
 * 
 * @author epalace
 *
 */
public class TestTupleThrift extends AbstractHadoopTestLibrary{
	
	@Test
	public void testSerialization() throws IOException, InterruptedException, ClassNotFoundException, GrouperException{
		A a = new A();
		a.setId("guay");
		a.setUrl("www.ewrwe");
		
		Tuple tuple = new Tuple();
		tuple.setConf(getConf());
		FieldsDescription schema = FieldsDescription.parse("name :string,age:int,risas : " +a.getClass().getName());
		tuple.setSchema(schema);
		tuple.setField("name","eric");
		tuple.setField("age",15);
		tuple.setField("risas",a);
		DataOutputBuffer output = new DataOutputBuffer();
		tuple.write(output);
		System.out.println("Serialized tuple " + tuple + " : " + output.getLength() + " bytes");
		
		Tuple tuple2 = new Tuple();
		tuple2.setConf(getConf());
		tuple2.setSchema(schema);
		
		DataInputBuffer input = new DataInputBuffer();
		input.reset(output.getData(),output.getLength());
		tuple2.readFields(input);
		
		for (Field field : schema.getFields()){
			String fieldName = field.getName();
			assertEquals(tuple.getField(fieldName),tuple2.getField(fieldName));
		}
		
		System.out.println("Deserialized tuple " + tuple2);
		assertEquals(tuple,tuple2);
	}
}
