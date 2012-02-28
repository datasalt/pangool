package com.datasalt.pangool.examples.topicalwordcount;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat.TupleInputReader;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat.TupleRecordWriter;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestTopicFingerprint extends AbstractHadoopTestLibrary {
	
	public static NullWritable nothing = NullWritable.get();

	public final static String INPUT = TestTopicFingerprint.class.getName() + "-input";
	public final static String OUTPUT = TestTopicFingerprint.class.getName() + "-output";
	
	@Test
	public void test() throws Exception {
		trash(OUTPUT);
		
		Configuration conf = new Configuration();
		
		createInput(INPUT, conf);
		ToolRunner.run( new TopicFingerprint(), new String[] {  INPUT, OUTPUT, 2 + "" } );
		
		TupleInputReader reader = new TupleInputReader(conf);
		reader.initialize(new Path(OUTPUT + "/part-r-00000"), conf);
		reader.nextKeyValueNoSync();
		ITuple tuple = reader.getCurrentKey();
		
		// The order in the output file is deterministic (we have sorted by topic, count)
		assertEquals(1, tuple.get("topic"));
		assertEquals("a", tuple.get("word").toString());
		
		reader.nextKeyValueNoSync();
		tuple = reader.getCurrentKey();
		
		assertEquals(1, tuple.get("topic"));
		assertEquals("c", tuple.get("word").toString());

		reader.nextKeyValueNoSync();
		tuple = reader.getCurrentKey();
		
		assertEquals(2, tuple.get("topic"));
		assertEquals("a", tuple.get("word").toString());

		reader.nextKeyValueNoSync();
		tuple = reader.getCurrentKey();
		
		assertEquals(2, tuple.get("topic"));
		assertEquals("b", tuple.get("word").toString());
		
		// Check the named output
	
		reader.close();
		reader = new TupleInputReader(conf);
		reader.initialize(new Path(OUTPUT + "/" + TopicFingerprint.OUTPUT_TOTALCOUNT + "/" + "part-r-00000"), conf);
		reader.nextKeyValueNoSync();
		tuple = reader.getCurrentKey();
		
		assertEquals(1, tuple.get("topic"));
		assertEquals(15, tuple.get("totalcount"));
		
		reader.nextKeyValueNoSync();
		tuple = reader.getCurrentKey();
		
		assertEquals(2, tuple.get("topic"));
		assertEquals(19, tuple.get("totalcount"));

		reader.close();
		
		trash(INPUT, OUTPUT);
	}
	
	public static TupleRecordWriter getTupleWriter(Configuration conf, String file, Schema schema) throws IOException {
		org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
		DataFileWriter<Record> writer = new DataFileWriter<Record>(new ReflectDatumWriter<Record>());
		writer.create(avroSchema, new File(file));
		return new TupleRecordWriter(avroSchema, schema, writer, new HadoopSerialization(conf));
	}
	
	public void createInput(String input, Configuration conf) throws IOException, InterruptedException {
		
		TupleRecordWriter writer = getTupleWriter(conf, input, TopicalWordCount.getSchema());

		// Topic 1, words: { a, 10 } { b, 1 } , { c, 5 }
		// Top 2 words = a(10), c(5)
		ITuple tuple = new Tuple(TopicalWordCount.getSchema());
		tuple.set("word", "a");
		tuple.set("topic", 1);
		tuple.set("count", 10);
		writer.write(tuple, nothing);
		
		tuple.set("word", "b");
		tuple.set("topic", 1);
		tuple.set("count", 1);
		writer.write(tuple, nothing);

		tuple.set("word", "c");
		tuple.set("topic", 1);
		tuple.set("count", 5);
		writer.write(tuple, nothing);
		
		// Topic 2, words: { a, 10 } { b, 9 } , { c, 5 }
		// Top 2 words = a(10), b(9)
		tuple.set("word", "a");
		tuple.set("topic", 2);
		tuple.set("count", 10);
		writer.write(tuple, nothing);
		
		tuple.set("word", "b");
		tuple.set("topic", 2);
		tuple.set("count", 9);
		writer.write(tuple, nothing);

		tuple.set("word", "c");
		tuple.set("topic", 2);
		tuple.set("count", 5);
		writer.write(tuple, nothing);

		writer.close(null);
	}
}
