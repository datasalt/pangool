package com.datasalt.pangolin.commons;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

/**
 * <p>The CoffeeJob accepts a text file as an input and does nothing with it. Its Mappers and Reducers just have a coffee.</p>
 * <p>This Job can be used to integration-test the Pangolin commons running infrastructure.</p>
 * 
 * @author pere
 *
 */
public class CoffeeJob extends BaseHadoopJob {

	final static Logger log = LoggerFactory.getLogger(CoffeeJob.class);
	
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			context.write(value, value);
		};
	}

	private static class Red extends Reducer<Text, Text, NullWritable, NullWritable> {

		@SuppressWarnings({ "rawtypes", "unused" })
    protected void setup(Context context) throws java.io.IOException ,InterruptedException {
			log.info("Just having a coffee."); // to test logging framework
      Multimap map = null; // use some external resource to test library synchronization
		};
	}
	
	@Override
	public Job getJob(String[] args, Configuration conf) throws IOException {
		if(args.length != 2) {
			throw new IllegalArgumentException("Number of args should be 2: [input] [output] (received " + args.length + ")");
		}
		Job job = new Job(conf, "Coffee Job");
		log.info("A capuccino, sir?");
		String input = args[0];
		Path output = new Path(args[1]);
		HadoopUtils.deleteIfExists(FileSystem.get(conf), output);
		job.setJarByClass(CoffeeJob.class);
		job.setMapperClass(CoffeeJob.Map.class);
		job.setReducerClass(CoffeeJob.Red.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, output);
		return job;  
	}
	
	public static void main(String args[]) throws Exception {
		BaseJob.main(CoffeeJob.class, args);
	}

	@Override
  public Properties getJobGeneratedProperties(Job job) {
	  return new Properties();
  }
}