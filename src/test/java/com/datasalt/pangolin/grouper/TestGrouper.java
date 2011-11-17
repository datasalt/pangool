package com.datasalt.pangolin.grouper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;


public class TestGrouper extends AbstractHadoopTestLibrary{

	private static class Mapy extends GrouperMapper<Text,NullWritable>{
		
		
		@Override
		public void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
		}
		
		
		@Override
		public void map(Text key,NullWritable value,Context context) throws IOException,InterruptedException{
			String[] tokens = key.toString().split("\\s+");
			String country = tokens[0];
			Integer age = Integer.parseInt(tokens[1]);
			String name = tokens[2];
			Integer height = Integer.parseInt(tokens[3]);
			
			Tuple outputKey = getTupleToEmit();
			outputKey.setField("country",country);
			outputKey.setField("age",age);
			outputKey.setField("name",name);
			outputKey.setField("height", height);
			emit(outputKey);
		}
	}
	
	private static class Red extends GrouperReducer<Tuple,NullWritable>{

//		@Override
//		public void reduce(Tuple key,Iterable<NullWritable> values,Context context) throws IOException,InterruptedException {
//			System.out.println("Principio reduce");
//			Iterator<NullWritable> iterator = values.iterator();
//			while(iterator.hasNext()){
//				NullWritable value2 = iterator.next();
//				Tuple tuple1 = context.getCurrentKey();
//				System.out.println("Tuple 1 " + tuple1);
////				NullWritable value = context.getCurrentValue();
////				
////				Tuple tuple2 = context.getCurrentKey();
////				System.out.println("Tuple 2" + tuple2);
//				
//				
//			}
//			System.out.println("Final reduce");
//			
//		}
		
		
		@Override
    public void onOpenGroup(int depth,Tuple firstElement, Context context) {
	    System.out.println("OPEN("+ depth +"):\t" + firstElement);
    }

		@Override
    public void onCloseGroup(int depth,Tuple lastElement, Context context) {
	    System.out.println("CLOSE:("+depth+")\t" + lastElement);
    }
		
		@Override
		public void onElements(Iterator<Tuple> tuples, Context context) throws IOException,InterruptedException {
			//System.out.println("start onElements");
			Iterator<Tuple> iterator = tuples;
			while ( iterator.hasNext()){
				Tuple tuple = iterator.next();
				System.out.println("element:\t" + tuple);
			}
			//System.out.println("end onElements");
		  
	  }
	}
	
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException, GrouperException{
		
		withInput("input",new Text("ES 20 listo 250"));
		withInput("input",new Text("US 14 perro 180"));
		withInput("input",new Text("US 14 perro 170"));
		withInput("input",new Text("US 14 beber 202"));
		withInput("input",new Text("US 15 jauja 160"));
		withInput("input",new Text("US 16 listo 160"));
		withInput("input",new Text("XE 20 listo 230"));
		
		Grouper grouper = new Grouper(getConf());
		grouper.setInputFormat(SequenceFileInputFormat.class);
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setMapperClass(Mapy.class);
		grouper.setReducerClass(Red.class);
		
		grouper.setSchema(Schema.parse("country:string , age:vint , name:string,height:int"));
		grouper.setSortCriteria("country ASC,age ASC");
		grouper.setMinGroup("country");
		grouper.setMaxGroup("country,age,name");
		
		grouper.setOutputKeyClass(Tuple.class);
		grouper.setOutputValueClass(NullWritable.class);
		
		
		Job job = grouper.getJob();
		FileInputFormat.setInputPaths(job,new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		assertRun(job);
	}
}
