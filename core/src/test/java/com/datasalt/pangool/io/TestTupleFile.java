package com.datasalt.pangool.io;

import com.datasalt.pangool.BaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class TestTupleFile extends BaseTest {

  public static String OUT = TestTupleFile.class.getName() + "-out";
  public static String IN = TestTupleFile.class.getName() + "-in";

  @Test
  public void testWriteAndRead() throws IOException {
    int numTuples = 10;
    ITuple tuples [] = new ITuple[numTuples];
    for(int i=0; i<numTuples; i++) {
      tuples[i] = fillTuple(true, new Tuple(SCHEMA));
    }

    FileSystem fs = FileSystem.get(getConf());
    TupleFile.Writer writer = new TupleFile.Writer(fs, getConf(), new Path(OUT), SCHEMA);
    for(ITuple tuple: tuples) {
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
}
