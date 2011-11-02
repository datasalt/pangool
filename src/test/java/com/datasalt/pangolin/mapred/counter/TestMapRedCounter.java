package com.datasalt.pangolin.mapred.counter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopTestUtils;
import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.test.PangolinBaseTest;
import com.datasalt.pangolin.io.LongPairWritable;


import com.datasalt.pangolin.mapred.counter.io.CounterDistinctKey;
import com.datasalt.pangolin.mapred.counter.io.CounterKey;

public class TestMapRedCounter extends PangolinBaseTest {
	
	public static final String OUTPUT_FOR_TEST = "test-" + TestMapRedCounter.class.getName();
	
	public static final String SINGLE_LINE_FILE = OUTPUT_FOR_TEST + "/singlelinefile.txt";
	
	public static final String OUTPUT_COUNT = OUTPUT_FOR_TEST + "/count";

	private FileSystem getFs() throws IOException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		return fs;
	}
	
	@Before
	public void setUp() throws Exception {
		HadoopTestUtils.oneLineTextFile(getFs(), new Path(SINGLE_LINE_FILE));
	}
	
	public static class TestMapper extends MapRedCounter.MapRedCounterMapper<LongWritable, Text> {

		Text t(String s) {
			return new Text(s);
		}
		
		@Override
    protected void map(LongWritable key, Text value,Context context) throws IOException,
        InterruptedException {
			
			emit( 0, t("single"), t("isingle"));
			
			emit( 1, t("c2d2"), t("a"));
			emit( 1, t("c2d2"), t("b"));

			emit( 1, t("c2d1"), t("a"));
			emit( 1, t("c2d1"), t("a"));
			
			emit( 1, t("c3d2"), t("a"));
			emit( 1, t("c3d2"), t("a"));
			emit( 1, t("c3d2"), t("b"));
			
			emit( 2, t("c4d3"), t("a"));
			emit( 2, t("c4d3"), t("b"));
			emit( 2, t("c4d3"), t("c"));
			emit( 2, t("c4d3"), t("c"));
						
			emit( 2, t("c6d3"), t("a"));
			emit( 2, t("c6d3"), t("a"));
			emit( 2, t("c6d3"), t("b"));
			emit( 2, t("c6d3"), t("b"));
			emit( 2, t("c6d3"), t("c"));
			emit( 2, t("c6d3"), t("c"));				
    }
		
	}
	
	@Test
	public void testWithCombiner() throws IOException, InterruptedException, ClassNotFoundException, CloneNotSupportedException {
		test(true);
	}
	
	@Test
	public void testWithoutCombiner() throws IOException, InterruptedException, ClassNotFoundException, CloneNotSupportedException {
		test(false);
	}

	public void test(boolean withCombiner) throws IOException, InterruptedException, ClassNotFoundException, CloneNotSupportedException {
		Configuration conf = getConf();
		Job job;
		
		if (withCombiner) {
			job = MapRedCounter.buildMapRedCounterJob("counter", SequenceFileOutputFormat.class, OUTPUT_COUNT, conf);
		} else {
			job = MapRedCounter.buildMapRedCounterJobWithoutCombiner("counter", SequenceFileOutputFormat.class, OUTPUT_COUNT, conf);
		}
		
		MapRedCounter.addInput(job, new Path(SINGLE_LINE_FILE), TextInputFormat.class, TestMapper.class);
		
		job.waitForCompletion(true);
		
		HashMap<String, Long> itemCount = itemCountAsMap(getFs(), OUTPUT_COUNT + "/" + MapRedCounter.Outputs.COUNTFILE + "-r-00000");		
		HashMap<String, LongPairWritable> itemGroupCount = itemGroupCountAsMap (getFs(), OUTPUT_COUNT + "/" + MapRedCounter.Outputs.COUNTDISTINCTFILE + "-r-00000");
		
		
		assertCount(1, "0:single:isingle", itemCount);		
		assertCount(1, "1:c2d2:a", itemCount);
		assertCount(1, "1:c2d2:b", itemCount);		
		assertCount(2, "1:c2d1:a", itemCount);
		assertCount(2, "1:c3d2:a", itemCount);
		assertCount(1, "1:c3d2:b", itemCount);
		assertCount(1, "2:c4d3:a", itemCount);
		assertCount(1, "2:c4d3:b", itemCount);
		assertCount(2, "2:c4d3:c", itemCount);
		
		
		
		
		
		assertGroupCount(1, 1, "0:single", itemGroupCount);
		assertGroupCount(2, 2, "1:c2d2", itemGroupCount);
		assertGroupCount(2, 1, "1:c2d1", itemGroupCount);
		assertGroupCount(3, 2, "1:c3d2", itemGroupCount);
		assertGroupCount(4, 3, "2:c4d3", itemGroupCount);
		assertGroupCount(6, 3, "2:c6d3", itemGroupCount);
	}
	
	private static void assertCount ( long count, String item, HashMap<String, Long> itemCount) {
		assertEquals(new Long(count), itemCount.get(item));
	}
	
	private static void assertGroupCount(long count, long distinct, String item, HashMap<String, LongPairWritable> groupCount) {
		assertEquals(count, groupCount.get(item).getValue1());
		assertEquals(distinct, groupCount.get(item).getValue2());		
	}
	
	/**
	 * Return a map with the counts for the items. Key: [typeIdentifier]:[group]:[item] 
	 */
	private HashMap<String, Long> itemCountAsMap(FileSystem fs, String file) throws IOException {
		HashMap<String, Long> m = new HashMap<String,Long> ();
		
		SequenceFile.Reader r = new SequenceFile.Reader(getFs(), new Path(file), getConf());
		
		CounterKey key = new CounterKey();
		LongWritable count = new LongWritable();
		while(r.next(key)) {
			r.getCurrentValue(count);
			m.put(key.getGroupId() + ":" + getSer().deser(new Text(), key.getGroup()) + ":" + getSer().deser(new Text(), key.getItem()), count.get());
		}
		
		return m;
	}
	
	/**
	 * Return a map with the counts for the items. Key: [typeIdentifier]:[group]
	 * @throws CloneNotSupportedException 
	 */
	private HashMap<String, LongPairWritable> itemGroupCountAsMap(FileSystem fs, String file) throws IOException, CloneNotSupportedException {
		HashMap<String, LongPairWritable> m = new HashMap<String,LongPairWritable> ();
		
		SequenceFile.Reader r = new SequenceFile.Reader(getFs(), new Path(file), getConf());
		
    CounterDistinctKey key = new CounterDistinctKey();
		LongPairWritable count = new LongPairWritable();
		while(r.next(key)) {
			r.getCurrentValue(count);
			m.put(key.getGroupId() + ":" + getSer().deser(new Text(), key.getGroup()), (LongPairWritable) count.clone());
		}
		
		return m;
	}

	
	@After
	public void tearDown() throws Exception {
		HadoopUtils.deleteIfExists(getFs(), new Path(OUTPUT_FOR_TEST));
	}


}
