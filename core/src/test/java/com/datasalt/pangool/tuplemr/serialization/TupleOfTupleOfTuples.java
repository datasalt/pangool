package com.datasalt.pangool.tuplemr.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

/**
 * Test that asserts that we can serialize Tuples inside Tuples and that there is no limit in the depth of such a tree of Tuples.
 */
public class TupleOfTupleOfTuples {

	final static Schema schema1 = new Schema("schema1", Fields.parse("a:int,b:string"));

	public static Schema getMetaSchema1() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("partition", Type.INT));
		fields.add(Fields.createTupleField("tuple", schema1));
		return new Schema("metaSchema1", fields);
	}

	public static Schema getMetaSchema2() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("group", Type.STRING));
		fields.add(Fields.createTupleField("metatuple", getMetaSchema1()));
		return new Schema("metaSchema2", fields);
	}

	@SuppressWarnings("serial")
	public static class MyHandler extends TupleMapper<LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {
			ITuple tuple = new Tuple(schema1);
			tuple.set("a", (int) (Math.random() * 1000));
			tuple.set("b", value.toString());

			ITuple mTuple = new Tuple(getMetaSchema1());
			mTuple.set("partition", (int) (Math.random() * 10));
			mTuple.set("tuple", tuple);

			ITuple mTuple2 = new Tuple(getMetaSchema2());
			mTuple2.set("group", value.toString());
			mTuple2.set("metatuple", mTuple);

			collector.write(mTuple2);
		}

	}

	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException, TupleMRException,
	    URISyntaxException {
		
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		
		Path out = new Path("out-" + TupleOfTupleOfTuples.class.getName());
		TupleMRBuilder builder = new TupleMRBuilder(conf);
		fS.delete(out, true);
		
		builder.setTupleOutput(out, getMetaSchema2());
		builder.addIntermediateSchema(getMetaSchema2());
		builder.addInput(new Path("src/test/resources/foo-file.txt"), new HadoopInputFormat(TextInputFormat.class), new MyHandler());
		builder.setGroupByFields("group");
		builder.setTupleReducer(new IdentityTupleReducer());
		Job job = builder.createJob();
		try {
			job.waitForCompletion(true);
		} finally {
			builder.cleanUpInstanceFiles();
		}

		Path toRead = new Path(out, "part-r-00000");
		assertTrue(fS.exists(toRead));
    TupleFile.Reader reader = new TupleFile.Reader(fS, conf, toRead);
    Tuple tuple = new Tuple(reader.getSchema());
			
		char base = 'a';
		for(int i = 0; i < 7; i++) {
			reader.next(tuple);
			assertEquals((char)(base + (char)i) + "", tuple.get("group").toString());
			assertEquals((char)(base + (char)i) + "", ((ITuple)(((ITuple)tuple.get("metatuple")).get("tuple"))).get("b").toString());
		}
		
		fS.delete(out, true);
	}
}
