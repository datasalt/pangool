package com.datasalt.pangool.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;

public class TestTupleFile extends BaseTest {

	public static String OUT = TestTupleFile.class.getName() + "-out";
	public static String OUT_2 = TestTupleFile.class.getName() + "-out-2";
	public static String OUT_3 = TestTupleFile.class.getName() + "-out-3";

	@Test
	public void testWriteAndRead() throws IOException {
		int numTuples = 10;
		ITuple tuples[] = new ITuple[numTuples];
		for(int i = 0; i < numTuples; i++) {
			tuples[i] = fillTuple(true, new Tuple(SCHEMA));
		}

		FileSystem fs = FileSystem.get(getConf());
		TupleFile.Writer writer = new TupleFile.Writer(fs, getConf(), new Path(OUT), SCHEMA);
		for(ITuple tuple : tuples) {
			writer.append(tuple);
		}
		writer.close();

		TupleFile.Reader reader = new TupleFile.Reader(fs, getConf(), new Path(OUT));
		Tuple inTuple = new Tuple(reader.getSchema());
		int count = 0;
		while(reader.next(inTuple)) {
			assertEquals(tuples[count++], inTuple);
		}
		reader.close();

		fs.delete(new Path(OUT), true);
	}
	
	@Test
	public void testBackwardsCompatibleReadWithDefaultValues() throws IOException {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("foo", Type.STRING, false));		
		Schema firstSchema = new Schema("first", fields);

		fields.add(Field.create("a", Type.INT, true, 10));
		fields.add(Field.create("b", Type.DOUBLE, true, 100d));
		fields.add(Field.create("c", Type.STRING, true, "foo"));
		fields.add(Field.create("d", Type.FLOAT, true, 10f));
		fields.add(Field.create("e", Type.BOOLEAN, true, true));
		fields.add(Field.create("f", Type.LONG, true, 100l));
		fields.add(Field.create("g", Type.BYTES, true, new byte[] { (byte)1, (byte)2 }));

		Schema secondSchema = new Schema("second", fields);
		
		int numTuples = 10;
		ITuple tuples[] = new ITuple[numTuples];
		for(int i = 0; i < numTuples; i++) {
			tuples[i] = new Tuple(firstSchema);
			tuples[i].set("foo", "str" + i);
		}
		
		FileSystem fs = FileSystem.get(getConf());
		TupleFile.Writer writer = new TupleFile.Writer(fs, getConf(), new Path(OUT_3), firstSchema);
		for(ITuple tuple : tuples) {
			writer.append(tuple);
		}
		writer.close();
		
		// Read it with an evolved schema
		TupleFile.Reader reader = new TupleFile.Reader(fs, secondSchema, getConf(), new Path(OUT_3));
		Tuple inTuple = new Tuple(secondSchema);
		int count = 0;
		while(reader.next(inTuple)) {
			assertEquals(tuples[count].getString("foo"), inTuple.getString("foo"));
			assertEquals(10, inTuple.get("a"));
			assertEquals(100d, inTuple.get("b"));
			assertEquals("foo", inTuple.get("c"));
			assertEquals(10f, inTuple.get("d"));
			assertEquals(true, inTuple.get("e"));
			assertEquals(100l, inTuple.get("f"));
			byte[] bts = (byte[])inTuple.get("g");
			assertEquals((byte)1, (byte)bts[0]);
			assertEquals((byte)2, (byte)bts[1]);
			count++;
		}
		reader.close();
	}

	@Test
	public void testBackwardsCompatibleWriteAndRead() throws IOException {
		Schema firstSchema = new Schema("first",
		    Fields.parse("a:int?, b:double, c:string?, d:float, e:boolean?, f:long, g:bytes"));
		Schema evolvedSchema = new Schema("evolved",
		    Fields.parse("b:double, d:float, e:boolean?, h:long?"));

		int numTuples = 10;
		ITuple tuples[] = new ITuple[numTuples];
		for(int i = 0; i < numTuples; i++) {
			tuples[i] = new Tuple(firstSchema);
			tuples[i].set("a", i);
			tuples[i].set("b", i + 0.5d);
			tuples[i].set("c", i + "" + i);
			tuples[i].set("d", i + 1.5f);
			tuples[i].set("e", i % 2 == 0);
			tuples[i].set("f", i + 1000l);
			tuples[i].set("g", new byte[] { (byte)0, (byte)1 });
		}

		FileSystem fs = FileSystem.get(getConf());
		TupleFile.Writer writer = new TupleFile.Writer(fs, getConf(), new Path(OUT_2), firstSchema);
		for(ITuple tuple : tuples) {
			writer.append(tuple);
		}
		writer.close();

		// Read it with an evolved schema
		TupleFile.Reader reader = new TupleFile.Reader(fs, evolvedSchema, getConf(), new Path(OUT_2));
		Tuple inTuple = new Tuple(evolvedSchema);
		int count = 0;
		while(reader.next(inTuple)) {
			assertEquals(tuples[count].get("b"), inTuple.get("b"));
			assertEquals(tuples[count].get("d"), inTuple.get("d"));
			assertEquals(tuples[count].get("e"), inTuple.get("e"));
			assertEquals(null, inTuple.get("h"));
			count++;
		}
		reader.close();
		
		// Read it with the schema that is in the file
		reader = new TupleFile.Reader(fs, getConf(), new Path(OUT_2));
		inTuple = new Tuple(reader.getSchema());
		count = 0;
		while(reader.next(inTuple)) {
			assertEquals(tuples[count].get("a"), inTuple.get("a"));
			assertEquals(tuples[count].get("b"), inTuple.get("b"));
			assertEquals(tuples[count].get("c"), inTuple.getString("c"));
			assertEquals(tuples[count].get("d"), inTuple.get("d"));
			assertEquals(tuples[count].get("e"), inTuple.get("e"));
			assertEquals(tuples[count].get("f"), inTuple.get("f"));
			byte[] bts = new byte[2];
			((ByteBuffer)inTuple.get("g")).get(bts);
			byte[] origBts = (byte[]) tuples[count].get("g");
			assertEquals(bts[0], origBts[0]);
			assertEquals(bts[1], origBts[1]);
			count++;
		}
		reader.close();

		fs.delete(new Path(OUT_2), true);
	}
}