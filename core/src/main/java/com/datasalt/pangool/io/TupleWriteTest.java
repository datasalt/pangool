package com.datasalt.pangool.io;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.mapred.AvroWrapper;
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

import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;
import com.google.common.io.Files;

public class TupleWriteTest {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			context.write(new Text(value.toString()), new Text(""));
		};
	}

	public static class Red extends Reducer<Text, Text, ITuple, NullWritable> {

		Tuple tuple = new Tuple();
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.getCounter("stats", "nRecords").increment(1);
			tuple.put("title", "foo");
			tuple.put("content", key.toString());
			context.write(tuple, NullWritable.get());
		};
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String input = "foo.txt";
		Files.write("foo1 bar1\nbar2 foo2", new File("foo.txt"), Charset.forName("UTF-8"));
		
		Path output = new Path("out-test");
		Configuration conf = new Configuration();
		conf.set("output_schema", "content:string, title:string"); // Pangool output schema
		
		conf.set("mapred.output.compress", "true");
		conf.set("mapred.output.compression", TupleOutputFormat.SNAPPY_CODEC);
		
		// Add Avro serialization
		Collection<String> serializations = conf.getStringCollection("io.serializations");
		if(!serializations.contains(AvroSerialization.class.getName())) {
			serializations.add(AvroSerialization.class.getName());
			conf.setStrings("io.serializations", serializations.toArray(new String[0]));
		}

		Job job = new Job(conf, "Using TupleOutputFormat");
		HadoopUtils.deleteIfExists(FileSystem.get(conf), output);
		job.setJarByClass(TupleWriteTest.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setReducerClass(Red.class);
		job.setOutputFormatClass(TupleOutputFormat.class);
		job.setOutputKeyClass(AvroWrapper.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}
}
