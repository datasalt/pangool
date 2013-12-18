package com.datasalt.pangool.tuplemr.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Mutator;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.serialization.HadoopSerialization;

public class TestSimpleTupleDeserializer {

	@Test
	public void test() throws IOException {
		Schema schema = new Schema("schema", Fields.parse("a:string, b:int?, c:double"));
		Schema targetSchema = new Schema("target", Fields.parse("a:string, b:int?, c:double, d:long?, e:boolean?"));
		
		Configuration conf = new Configuration();
		HadoopSerialization hadoopSerDe = new HadoopSerialization(conf);

		ITuple tuple = new Tuple(schema);
		tuple.set("a", "foo");
		tuple.set("b", 10);
		tuple.set("c", 5d);
		
		SimpleTupleSerializer ser = new SimpleTupleSerializer(schema, hadoopSerDe, conf);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ser.open(bos);
		
		for(int i = 0; i < 10; i++) {
			ser.serialize(tuple);
		}
		
		ser.close();
		
		bos.close();
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		
		SimpleTupleDeserializer des = new SimpleTupleDeserializer(schema, targetSchema, hadoopSerDe, conf);
		des.open(bis);
		
		ITuple targetTuple = new Tuple(targetSchema);
		for(int i = 0; i < 10; i++) {
			des.deserialize(targetTuple);
		}
		
		assertEquals("foo", targetTuple.getString("a"));
		assertEquals(10, targetTuple.get("b"));
		assertEquals(5d, targetTuple.get("c"));
		assertNull(targetTuple.get("d"));
		assertNull(targetTuple.get("e"));
		
		// Something important is that if we read a file that doesn't contains a field
		// just after a file that contains this field, we should clear the field even
		// in the case that no default value was provided.
		schema = new Schema("schema", Fields.parse("a:string, c:double"));
		tuple = new Tuple(schema);
		tuple.set("a", "foo");
		tuple.set("c", 5d);
		
		bos = new ByteArrayOutputStream();
		ser = new SimpleTupleSerializer(schema, hadoopSerDe, conf);
		ser.open(bos);
		
		for(int i = 0; i < 10; i++) {
			ser.serialize(tuple);
		}
		
		ser.close();
		bos.close();		
		bis = new ByteArrayInputStream(bos.toByteArray());		
		des = new SimpleTupleDeserializer(schema, targetSchema, hadoopSerDe, conf);
		des.open(bis);
		
		for(int i = 0; i < 10; i++) {
			des.deserialize(targetTuple);
		}
		
		assertEquals("foo", targetTuple.getString("a"));
		assertNull(targetTuple.get("b"));
		assertEquals(5d, targetTuple.get("c"));
		assertNull(targetTuple.get("d"));
		assertNull(targetTuple.get("e"));
		
		bis.close();
	}
	
	@Test
	public void testDefaultValuesPerformance() throws IOException {
		Field cField = Field.create("c", Type.DOUBLE, true, 100d);
		Field dField = Field.create("d", Type.LONG, true, 1000l);
		
		Schema schema = new Schema("schema", Fields.parse("a:string, b:int?"));
		Schema targetSchema = Mutator.superSetOf(schema, cField, dField);
		
		Configuration conf = new Configuration();
		HadoopSerialization hadoopSerDe = new HadoopSerialization(conf);

		ITuple tuple = new Tuple(schema);
		tuple.set("a", "foo");
		tuple.set("b", 10);
		
		SimpleTupleSerializer ser = new SimpleTupleSerializer(schema, hadoopSerDe, conf);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ser.open(bos);
		
		for(int i = 0; i < 100000; i++) {
			ser.serialize(tuple);
		}
		
		ser.close();
		
		bos.close();
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		
		SimpleTupleDeserializer des = new SimpleTupleDeserializer(schema, targetSchema, hadoopSerDe, conf);
		des.open(bis);
		
		ITuple targetTuple = new Tuple(targetSchema);
		long start = System.currentTimeMillis();
		for(int i = 0; i < 100000; i++) {
			des.deserialize(targetTuple);
		}
		long end = System.currentTimeMillis();
		System.out.println(end - start);
		
		assertEquals("foo", targetTuple.getString("a"));
		assertEquals(10, targetTuple.get("b"));
		assertEquals(100d, targetTuple.get("c"));
		assertEquals(1000l, targetTuple.get("d"));
	}
}
