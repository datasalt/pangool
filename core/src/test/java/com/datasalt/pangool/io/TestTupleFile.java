package com.datasalt.pangool.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.pangool.BaseTest;

public class TestTupleFile extends BaseTest {

	public static String OUT = TestTupleFile.class.getName() + "-out";
	public static String OUT_2 = TestTupleFile.class.getName() + "-out-2";

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