package com.datasalt.pangolin.grouper;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.Schema.Field;
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
		Schema schema = Schema.parse("name :string,age:int,risas : " +a.getClass().getName());
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
