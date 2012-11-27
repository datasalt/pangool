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
package com.datasalt.pangool.examples.topicalwordcount;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTopicFingerprint extends AbstractHadoopTestLibrary {

	public final static String INPUT = TestTopicFingerprint.class.getName() + "-input";
	public final static String OUTPUT = TestTopicFingerprint.class.getName() + "-output";
	
	@Test
	public void test() throws Exception {
		trash(OUTPUT);
		
		Configuration conf = new Configuration();
		
		createInput(INPUT, conf);
		ToolRunner.run(getConf(), new TopicFingerprint(), new String[] {  INPUT, OUTPUT, 2 + "" } );

    Path outPath = new Path(OUTPUT + "/part-r-00000");
    FileSystem fs = FileSystem.get(outPath.toUri(), conf);
    TupleFile.Reader reader = new TupleFile.Reader(fs, conf, outPath);
    Tuple tuple = new Tuple(reader.getSchema());

		// The order in the output file is deterministic (we have sorted by topic, count)
    reader.next(tuple);
		assertEquals(1, tuple.get("topic"));
		assertEquals("a", tuple.get("word").toString());
		
		reader.next(tuple);
		assertEquals(1, tuple.get("topic"));
		assertEquals("c", tuple.get("word").toString());

    reader.next(tuple);
		assertEquals(2, tuple.get("topic"));
		assertEquals("a", tuple.get("word").toString());

    reader.next(tuple);
		assertEquals(2, tuple.get("topic"));
		assertEquals("b", tuple.get("word").toString());
		
		// Check the named output
	
		reader.close();
    outPath = new Path(OUTPUT + "/" + TopicFingerprint.OUTPUT_TOTALCOUNT + "/" + "part-r-00000");
		reader = new TupleFile.Reader(fs, conf, outPath);
    tuple = new Tuple(reader.getSchema());

    reader.next(tuple);
		assertEquals(1, tuple.get("topic"));
		assertEquals(15, tuple.get("totalcount"));

    reader.next(tuple);
		assertEquals(2, tuple.get("topic"));
		assertEquals(19, tuple.get("totalcount"));

		reader.close();
		
		trash(INPUT, OUTPUT);
	}
	

	public void createInput(String input, Configuration conf) throws IOException, InterruptedException {
    Path inPath = new Path(input);
    FileSystem fs = FileSystem.get(inPath.toUri(), conf);
		TupleFile.Writer writer = new TupleFile.Writer(fs, conf, inPath, TopicalWordCount.getSchema());

		// Topic 1, words: { a, 10 } { b, 1 } , { c, 5 }
		// Top 2 words = a(10), c(5)
		ITuple tuple = new Tuple(TopicalWordCount.getSchema());
		tuple.set("word", "a");
		tuple.set("topic", 1);
		tuple.set("count", 10);
		writer.append(tuple);
		
		tuple.set("word", "b");
		tuple.set("topic", 1);
		tuple.set("count", 1);
    writer.append(tuple);

		tuple.set("word", "c");
		tuple.set("topic", 1);
		tuple.set("count", 5);
    writer.append(tuple);
		
		// Topic 2, words: { a, 10 } { b, 9 } , { c, 5 }
		// Top 2 words = a(10), b(9)
		tuple.set("word", "a");
		tuple.set("topic", 2);
		tuple.set("count", 10);
    writer.append(tuple);
		
		tuple.set("word", "b");
		tuple.set("topic", 2);
		tuple.set("count", 9);
    writer.append(tuple);

		tuple.set("word", "c");
		tuple.set("topic", 2);
		tuple.set("count", 5);
    writer.append(tuple);

		writer.close();
	}
}
