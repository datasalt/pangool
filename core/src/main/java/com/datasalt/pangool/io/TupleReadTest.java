package com.datasalt.pangool.io;

import java.io.IOException;
import java.util.Collection;

import org.apache.avro.mapred.AvroSerialization;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.commons.HadoopUtils;
import com.datasalt.pangool.io.tuple.ITuple;

public class TupleReadTest {

	public static class TupleMap extends Mapper<ITuple, NullWritable, Text, Text> {

		protected void map(ITuple key, NullWritable value, Context context) throws IOException ,InterruptedException {
			
			context.write(new Text(key.getString("title")), new Text(key.getString("content")));
		};
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String input = "out-test";
		Path output = new Path("out-test-2");
		Configuration conf = new Configuration();

		// Add Avro serialization
		Collection<String> serializations = conf.getStringCollection("io.serializations");
		if(!serializations.contains(AvroSerialization.class.getName())) {
			serializations.add(AvroSerialization.class.getName());
			conf.setStrings("io.serializations", serializations.toArray(new String[0]));
		}

		Job job = new Job(conf, "Using TupleInputFormat");
		HadoopUtils.deleteIfExists(FileSystem.get(conf), output);
		job.setJarByClass(TupleReadTest.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(TupleMap.class);
			
		job.setInputFormatClass(TupleInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}
}
