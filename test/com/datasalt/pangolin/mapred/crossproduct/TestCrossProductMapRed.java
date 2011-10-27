package com.datasalt.pangolin.mapred.crossproduct;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangolin.commons.HadoopUtils;
import com.datasalt.pangolin.commons.count.Counter;
import com.datasalt.pangolin.commons.count.Counter.Count;
import com.datasalt.pangolin.commons.test.BaseTest;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.mapred.crossproduct.CrossProductMapRed;
import com.datasalt.pangolin.mapred.crossproduct.CrossProductMapRed.CrossProductMapper;
import com.datasalt.pangolin.mapred.crossproduct.io.CrossProductExtraKey;
import com.datasalt.pangolin.mapred.crossproduct.io.CrossProductPair;

/**
 * Unit test for the {@link CrossProductMapRed}. It performs two tests: one in which we need 1 Map/Red and one in which
 * we need 2 because we say that we can only fit lists of one element in memory.
 * 
 * @author pere
 *
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestCrossProductMapRed  extends BaseTest {

	public final static String INPUT_1  = "test-1-"     + TestCrossProductMapRed.class + ".txt";
	public final static String INPUT_2  = "test-2-"     + TestCrossProductMapRed.class + ".txt";
	public final static String OUTPUT   = "test-out-"   + TestCrossProductMapRed.class;
	
	Serialization ser; 
	
	@Before
	public void startUp() throws IOException {
		ser = new Serialization(getConf());
	}
	
	@Test
	public void testWithTwoSteps() throws Exception {
		test(true);
	}
	
	@Test
	public void testJustOneStep() throws Exception {
		test(false);
	}
	
	
	public void test(boolean twoSteps) throws Exception {
		createFirstDataSet();
		createSecondDataSet();
		
		Configuration conf = getConf();
		FileSystem fS = FileSystem.get(conf);

		if(twoSteps) {
			/*
			 * Here we are saying that 1-list elements is enough to collapse the JVM's heap - just for testing 
			 */
			conf.setInt(CrossProductMapRed.SPLIT_DATASET_SIZE_CONF, 1);
		}
		
		
		CrossProductMapRed crossProduct = new CrossProductMapRed("Test",conf);
		crossProduct.setLeftInputPath(new Path(INPUT_1));
		crossProduct.setLeftInputFormat(TextInputFormat.class);
		crossProduct.setLeftInputMapper(Map.class);
		
		crossProduct.setRightInputPath(new Path(INPUT_2));
		crossProduct.setRightInputFormat(TextInputFormat.class);
		crossProduct.setRightInputMapper(Map.class);
		
		crossProduct.setOutputPath(new Path(OUTPUT));
		crossProduct.setOutputFormat(SequenceFileOutputFormat.class);
		
		
		crossProduct.memoryAwareRun();
		SequenceFile.Reader reader;
		CrossProductExtraKey groupKey = new CrossProductExtraKey();
		CrossProductPair data   = new CrossProductPair();		
		Text txt  = new Text();
		Text txt2 = new Text();
		if(twoSteps) {

			reader = new SequenceFile.Reader(fS, new Path(OUTPUT, "EXTRA-r-00000"), conf);
	
			/*
			 * Assert intermediate "big groups" output
			 */
			for(int i = 0; i < 9; i++) {
				reader.next(groupKey);
				reader.getCurrentValue(data);
			
				if(i < 3) {
					ser.deser(txt, data.getRight());
					switch(i) {
					case 0: assertEquals(txt.toString(), "pere"); break;
					case 1: assertEquals(txt.toString(), "eric"); break;
					case 2: assertEquals(txt.toString(), "ivan"); break;
					}
				} else { 
					ser.deser(txt, data.getLeft());
					switch(i) {
					case 3: assertEquals(txt.toString(), "beer"); break;
					case 4: assertEquals(txt.toString(), "beer"); break;
					case 5: assertEquals(txt.toString(), "beer"); break;
					case 6: assertEquals(txt.toString(), "wine"); break;
					case 7: assertEquals(txt.toString(), "wine"); break;
					case 8: assertEquals(txt.toString(), "wine"); break;
					}
				}
			}
			
			reader.close();
		}

		/*
		 * Assert final output
		 */
		
		Counter count = Counter.createWithDistinctElements();
		
		Path finalOutput = new Path(OUTPUT, "part-r-00000");
		if(twoSteps) {
			finalOutput = new Path(crossProduct.getBigGroupsOutput(), "part-r-00000");
		}
		reader = new SequenceFile.Reader(fS, finalOutput, conf);

		for(int i = 0; i < 6; i++) {
			reader.next(data);
			ser.deser(txt, data.getLeft());
			ser.deser(txt2, data.getRight());
			count.in(txt.toString()).count(txt2.toString());
		}
		
		Count counts = count.getCounts();
		List<String> beerResults = counts.get("beer").getDistinctListAsStringList();
		List<String> wineResults = counts.get("wine").getDistinctListAsStringList();
		for(List<String> list : new List[] { beerResults, wineResults }) {
			assertEquals(list.contains("pere"), true);
			assertEquals(list.contains("ivan"), true);
			assertEquals(list.contains("eric"), true);
		}
		
		HadoopUtils.deleteIfExists(fS, new Path(INPUT_1));
		HadoopUtils.deleteIfExists(fS, new Path(INPUT_2));
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
		if(twoSteps) {
			HadoopUtils.deleteIfExists(fS, crossProduct.getBigGroupsOutput());
		}
	}
	
	final static String GROUP = "1";
	
	public static class Map extends CrossProductMapper<LongWritable, Text, Text> {
		
		protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
			
			emit(GROUP, value);
		};
	}
	
  private void createFirstDataSet() throws IOException, TException {
  	BufferedWriter writer = new BufferedWriter(new FileWriter(INPUT_1));
  	writer.write("beer" + "\n");
  	writer.write("wine" + "\n");
  	writer.close();
  }
  
  private void createSecondDataSet() throws IOException {
  	BufferedWriter writer = new BufferedWriter(new FileWriter(INPUT_2));
  	writer.write("pere" + "\n");
  	writer.write("eric" + "\n");
  	writer.write("ivan" + "\n");
  	writer.close();
	}
}