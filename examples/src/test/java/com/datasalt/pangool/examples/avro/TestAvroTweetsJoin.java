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
package com.datasalt.pangool.examples.avro;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
/**
 * 
 * Tests for {@link AvroTweetsJoin}
 *
 */
public class TestAvroTweetsJoin extends AbstractHadoopTestLibrary {
	
	private final static String INPUT_TWEETS = 
			TestAvroTweetsJoin.class.getName() + "-input-tweets"+AvroOutputFormat.EXT;
	private final static String INPUT_RETWEETS = 
			TestAvroTweetsJoin.class.getName() + "-input-retweets";
	private final static String OUTPUT = 
			TestAvroTweetsJoin.class.getName() + "-output"+AvroOutputFormat.EXT;
	
	@Test
	public void test() throws Exception {
		createTweetInput(INPUT_TWEETS);
		createRetweetsInput(INPUT_RETWEETS);
		ToolRunner.run( new AvroTweetsJoin(), new String[] {  INPUT_TWEETS,INPUT_RETWEETS,OUTPUT });
		assertOutput(OUTPUT + "/part-r-00000.avro", getConf());
		trash(INPUT_TWEETS,INPUT_RETWEETS,OUTPUT);
	}
	
	public static void assertOutput(String output, Configuration conf) 
			throws NumberFormatException, IOException, InterruptedException {

		DatumReader datumReader = new SpecificDatumReader(AvroTweetsJoin.getAvroOutputSchema());
		FileReader reader = DataFileReader.openReader(new File(output),datumReader);
		
		Record record=null;
		record = (Record)reader.next(record);
		
		Assert.assertEquals("eric",record.get("username").toString());
		Array<Utf8> hashtags = (Array<Utf8>)record.get("hashtags");
		assertEquals(hashtags,"ivan","datasalt","pere");
		
		reader.next(record);
		Assert.assertEquals("eric",record.get("username").toString());
		assertEquals(hashtags,"ivan2","datasalt2","pere2");
		reader.next(record);
		Assert.assertEquals("marianico",record.get("username").toString());
		assertEquals(hashtags,"ivan2","datasalt2","pere2");
		
		Assert.assertFalse(reader.hasNext());
		
	}
	
	private static Array<Utf8> toArrayUtf8(String ... strings ){
		List<Utf8> list = new ArrayList<Utf8>();
		for (String s : strings){
			list.add(new Utf8(s));
		}
		org.apache.avro.Schema schema = AvroTweetsJoin.getAvroStringArraySchema();
		return new Array<Utf8>(schema,list);
	}
	
	private static void assertEquals(Array<Utf8> actual, String ... expectedStrings){
		Array<Utf8> expected = toArrayUtf8(expectedStrings);
		Assert.assertEquals(expected, actual);
	}
	
	public static void createTweetInput(String where) throws IOException {
		
		new File(where).delete();
		
		DataFileWriter<Record> writer =
	      new DataFileWriter<Record>(new ReflectDatumWriter<Record>());
		
		CodecFactory factory = CodecFactory.deflateCodec(1);
		org.apache.avro.Schema schema = AvroTweetsJoin.getAvroTweetSchema();
		
    writer.setCodec(factory);
    int SYNC_SIZE = 16;
    int DEFAULT_SYNC_INTERVAL = 1000*SYNC_SIZE; 
		writer.setSyncInterval(DEFAULT_SYNC_INTERVAL);
    writer.create(schema,new File(where));
    Record record = new Record(schema);
    record.put("id",1);
    record.put("text","1");
    record.put("timestamp",1L);
    record.put("hashtags",new String[]{"ivan","datasalt","pere"});
    writer.append(record);
    
    record.put("id",2);
    record.put("text","2");
    record.put("timestamp",2L);
    record.put("hashtags",new String[]{"ivan2","datasalt2","pere2"});
    writer.append(record);

		writer.close();
	}
	
	public static void createRetweetsInput(String filename) throws IOException {
		File file = new File(filename);
		file.delete();
		FileWriter writer = new FileWriter(file);
		// retwetter  / tweet
		writer.append("eric\t1\n");
		writer.append("eric\t2\n");
		writer.append("marianico\t2\n");
		writer.close();
	}
}
