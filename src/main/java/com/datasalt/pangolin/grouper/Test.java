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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.Tuple.NoSuchFieldException;
import com.datasalt.pangolin.grouper.mapred.GrouperMapper;
import com.datasalt.pangolin.grouper.mapred.GrouperWithRollupReducer;


public class Test extends AbstractHadoopTestLibrary {

	private static class Mapy extends GrouperMapper<Text,NullWritable>{
		
		
		@Override
		public void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
		}
		
		
		@Override
		public void map(Text key,NullWritable value,Context context) throws IOException,InterruptedException{
			try{
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
			} catch(NoSuchFieldException e){
				throw new RuntimeException(e);
			}
		}
	}
	
	private static class Red extends GrouperWithRollupReducer<Text,NullWritable>{

		@Override
    public void onOpenGroup(int depth,String field,Tuple firstElement, Context context) throws IOException,InterruptedException{
			Text text = new Text("OPEN("+ depth+","+field +"):\t\t" + firstElement);
	    context.write(text,NullWritable.get());
    }

		@Override
    public void onCloseGroup(int depth,String field,Tuple lastElement, Context context) throws IOException,InterruptedException {
	    Text text = new Text("CLOSE:("+depth+","+field+")\t\t" + lastElement);
			context.write(text,NullWritable.get());
    }
		
		@Override
		public void onElements(Iterable<Tuple> tuples, Context context) throws IOException,InterruptedException {
			Iterator<Tuple> iterator = tuples.iterator();
			Text text = new Text();
			while ( iterator.hasNext()){
				Tuple tuple = iterator.next();
				text.set("element:\t\t" + tuple);
				context.write(text,NullWritable.get());
			}
	  }
	}
	
	
	
	public void test() throws IOException, InterruptedException, ClassNotFoundException, GrouperException{
		
		withInput("input",new Text("ES 20 listo 250"));
		withInput("input",new Text("US 14 perro 180"));
		withInput("input",new Text("US 14 perro 170"));
		withInput("input",new Text("US 14 beber 202"));
		withInput("input",new Text("US 15 jauja 160"));
		withInput("input",new Text("US 16 listo 160"));
		withInput("input",new Text("XE 20 listo 230"));
		
		GrouperWithRollup grouper = new GrouperWithRollup(getConf());
		grouper.setInputFormat(SequenceFileInputFormat.class);
		grouper.setOutputFormat(TextOutputFormat.class);
		grouper.setMapperClass(Mapy.class);
		grouper.setReducerClass(Red.class);
		
		grouper.setSchema(FieldsDescription.parse("country:string , age:vint , name:string,height:int"));
		grouper.setSortCriteria("country ASC,age ASC");
		grouper.setMinGroup("country");
		grouper.setMaxGroup("country,age,name");
		
		grouper.setOutputKeyClass(Tuple.class);
		grouper.setOutputValueClass(NullWritable.class);
		
		
		Job job = grouper.getJob();
		job.setNumReduceTasks(2);
		FileInputFormat.setInputPaths(job,new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		assertRun(job);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, GrouperException{
		Test test = new Test();
		test.initHadoop();
		test.test();
	}
	
}
