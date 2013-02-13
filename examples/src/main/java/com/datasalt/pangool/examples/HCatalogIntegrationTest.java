package com.datasalt.pangool.examples;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HCatTupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

@SuppressWarnings("serial")
public class HCatalogIntegrationTest implements Serializable, Tool {

	private transient Configuration conf;
	
  public static void main(String[] args) throws Exception {
		ToolRunner.run(new HCatalogIntegrationTest(), args);
	}

	@Override
  public Configuration getConf() {
	  return null;
  }

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
  }

	@Override
  public int run(String[] args) throws Exception {
  	// Re. environment variables: http://incubator.apache.org/hcatalog/docs/r0.4.0/inputoutput.html
		if(args.length != 2) {
			System.err.println("Number of args must be 2.");
			System.out.println("Usage: HCatTupleInputFormat dbName tableName");
			System.out.println();
			System.out.println();
			System.out.println("Reads an HCatalog Table an prints the resultant Tuples to System.out . Can be used to integrate-test the HCataTupleInputFormat.");
			System.out.println("Please refer to: http://incubator.apache.org/hcatalog/docs/r0.4.0/inputoutput.html regarding environment variables, etc. You must have HCatalog installed and running with Hive + MySQL.");
			return -1;
		}
		
		String dbName = args[0];
		String tableName = args[1];
		
		MapOnlyJobBuilder builder = new MapOnlyJobBuilder(conf, "HCatTupleInputFormat Integration Test");
		// input path can't be null in Pangool so we enter anything
		builder.addInput(new Path("anything"), new HCatTupleInputFormat(dbName, tableName, conf), new MapOnlyMapper<ITuple, NullWritable, NullWritable, NullWritable>() {
			
			protected void map(ITuple key, NullWritable value, Context context) throws IOException, InterruptedException {
				System.out.println(key.toString());
			};
		});
		builder.setOutput(new Path(HCatTupleInputFormat.class + "-out"), new HadoopOutputFormat(NullOutputFormat.class), NullWritable.class, NullWritable.class);
		builder.createJob().waitForCompletion(true);
	  return 1;
  }
}
