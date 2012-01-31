package com.datasalt.avrool.examples.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.commons.HadoopUtils;




public class PlainAvroWordCount {


	@SuppressWarnings("serial")
	private static class Split extends Mapper<LongWritable, Text,AvroKey<String>,AvroValue<Integer>> {

		
		private AvroKey<String> outputKey = new AvroKey<String>();
		private AvroValue<Integer> outputValue = new AvroValue<Integer>(1);
		
		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			
			
			while(itr.hasMoreTokens()) {
				outputKey.datum(itr.nextToken());
				context.write(outputKey,outputValue);
			}
		}
	}

	@SuppressWarnings("serial")
	private static class CountCombiner extends Reducer<AvroKey<Utf8>,AvroValue<Integer>,AvroKey<Utf8>,AvroValue<Integer>> {
		
		private AvroValue<Integer> outputValue = new AvroValue<Integer>();
		
		@Override
		public void reduce(AvroKey<Utf8> key, Iterable<AvroValue<Integer>> values, Context context) throws IOException,InterruptedException {

			int count = 0;
			for(AvroValue<Integer> value : values) {
				count += (Integer)value.datum();
			}

			this.outputValue.datum(count);
			context.write(key,outputValue);
		}
	}
	
	@SuppressWarnings("serial")
	private static class CountReducer extends Reducer<AvroKey<Utf8>,AvroValue<Integer>,Text,IntWritable> {
		
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		
		@Override
		public void reduce(AvroKey<Utf8> key, Iterable<AvroValue<Integer>> values, Context context) throws IOException,InterruptedException {

			int count = 0;
			for(AvroValue<Integer> value : values) {
				count += (Integer)value.datum();
			}
			
			Utf8 utf8 = key.datum();
			outputKey.set(utf8.getBytes(),0,utf8.getByteLength());
			//outputKey.set(key.datum().toString());
			outputValue.set(count);
			context.write(outputKey,outputValue);
		}
	}
	
	

	

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException{
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);

		Schema keySchema = Schema.create(Type.STRING);
		Schema valueSchema = Schema.create(Type.INT);
		
		JobConf jobConf = new JobConf(conf);
		
		Schema interSchema = Pair.getPairSchema(keySchema, valueSchema);
		AvroJob.setMapOutputSchema(jobConf, interSchema);
		
		Job job = new Job(jobConf,"Plain Avro Word Count");
		
		
		job.setJarByClass(PlainAvroWordCount.class);
		job.setMapperClass(Split.class);
		job.setCombinerClass(CountCombiner.class);
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VIntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		
		Path outputPath = new Path(output);
		HadoopUtils.deleteIfExists(fs, outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		return job;
	}

	private static final String HELP = "Usage: PlainAvroWordCount [input_path] [output_path]";

	public static void main(String args[]) throws CoGrouperException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 2) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new PlainAvroWordCount().getJob(conf, args[0], args[1]).waitForCompletion(true);
	}
}

