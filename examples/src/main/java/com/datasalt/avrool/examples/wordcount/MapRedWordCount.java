package com.datasalt.avrool.examples.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.datasalt.avrool.commons.HadoopUtils;

public class MapRedWordCount {

	private static class TokenizerMapper extends Mapper<Object, Text, Text, VIntWritable> {

		private final static VIntWritable one = new VIntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	private static class IntSumReducer extends Reducer<Text,VIntWritable, Text, VIntWritable> {

		private VIntWritable result = new VIntWritable();

		public void reduce(Text key, Iterable<VIntWritable> values, Context context) throws IOException,
		    InterruptedException {
			int sum = 0;
			for(VIntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(MapRedWordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VIntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		Path outputPath = new Path(otherArgs[1]);
		HadoopUtils.deleteIfExists(fs, outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
