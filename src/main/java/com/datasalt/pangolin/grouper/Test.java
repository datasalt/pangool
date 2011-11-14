package com.datasalt.pangolin.grouper;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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



public class Test {

	
	public static class Mappy extends Mapper<LongWritable,Text,Tuple,NullWritable>{
		
		@Override
		public void setup(Context context){
			
		}
		
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
			Tuple n = new Tuple();
			
		    context.write(n,NullWritable.get());
    
		}
		
		@Override
		public void cleanup(Context context){
			
		}
		
		
		
	}
	
	public static class Red extends Reducer<Tuple,NullWritable,Tuple,NullWritable>{
		
		@Override
		public void setup(Context context){
			
		}
		
		@Override
		public void cleanup(Context context){
			
		}
		
		
		@Override
		public void run(Context context) throws IOException,InterruptedException{
			super.run(context);
//			context.nextKeyValue();
//			System.out.println("RUN");
			
		}
		
		@Override
		public void reduce(Tuple key,Iterable<NullWritable> values,Context context) throws IOException,InterruptedException{
			
		}
		
		
		
		
		
	}
	
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job = new org.apache.hadoop.mapreduce.Job(conf);
		job.setMapperClass(Mappy.class);
		job.setReducerClass(Red.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job,new Path("caca.txt"));
		FileOutputFormat.setOutputPath(job,new Path("cacuza" +System.currentTimeMillis()));
		
		
		job.waitForCompletion(true);
		
	}
	
	
}
