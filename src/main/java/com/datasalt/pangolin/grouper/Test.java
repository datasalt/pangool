package com.datasalt.pangolin.grouper;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.commons.HadoopUtils;



public class Test {

	
	public static class Mappy extends Mapper<LongWritable,Text,Tuple,NullWritable>{
		
		private Schema schema;
		private Tuple outputKey;
		private Random random;
		private byte[] bytes = new byte[8];
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			  super.setup(context);
      this.schema = Grouper.getSchema(context.getConfiguration());
      outputKey = ReflectionUtils.newInstance(Tuple.class,context.getConfiguration());
      random = new Random();
      TupleComparator comp = (TupleComparator)WritableComparator.get(Tuple.class);
      comp.setConf(context.getConfiguration());
      
			
		}
		
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
				for (int i=0 ; i < 10000; i++){
					outputKey.setField("userId", random.nextInt());
					outputKey.setField("age", random.nextLong());
					random.nextBytes(bytes);
					outputKey.setField("name",new String(bytes));
					context.write(outputKey,NullWritable.get());
				}
		}
		
		@Override
		public void cleanup(Context context) throws IOException,InterruptedException{
			super.cleanup(context);
		}
		
		public void run(Context context) throws IOException,InterruptedException {
			super.run(context);
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

			
		}
		
		@Override
		public void reduce(Tuple key,Iterable<NullWritable> values,Context context) throws IOException,InterruptedException{
			super.reduce(key,values,context);
		}
		
		
		
		
		
	}
	
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		
		
		Configuration conf = new Configuration();
		conf.set(Grouper.CONF_SCHEMA,"name:string,userId:vint,age:vlong");
		conf.set(Grouper.CONF_FIELDS_GROUP,"userId");
		TupleComparator blabla = new TupleComparator(); //in order to load static block
		Job job = new Job(conf);
		job.setMapperClass(Mappy.class);
		job.setReducerClass(Red.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setGroupingComparatorClass(TupleComparator.class);
		job.setPartitionerClass(TuplePartitioner.class);
		FileInputFormat.addInputPath(job,new Path("caca.txt"));
		Path output = new Path("cacuza");
		FileOutputFormat.setOutputPath(job,output);
		HadoopUtils.deleteIfExists(FileSystem.get(conf), output);
		job.waitForCompletion(true);
		
	}
	
	
}
