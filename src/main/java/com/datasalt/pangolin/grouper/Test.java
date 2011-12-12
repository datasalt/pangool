package com.datasalt.pangolin.grouper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleImpl;
import com.datasalt.pangolin.grouper.io.TupleImpl.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapred.GrouperMapperHandler;
import com.datasalt.pangolin.grouper.mapred.GrouperReducerHandler;


public class Test extends AbstractHadoopTestLibrary {

	private static class Mapy extends GrouperMapperHandler<Text,NullWritable>{
		
		//private Configuration conf;
		private TupleImpl outputKey;
		@Override
		public void setup(Mapper.Context context) throws IOException,InterruptedException {
			super.setup(context);
			try {
	      FieldsDescription schema = FieldsDescription.parse(context.getConfiguration());
      } catch(GrouperException e) {
	      throw new RuntimeException(e);
      }
		}
		
		
		@Override
		public void map(Text key,NullWritable value) throws IOException,InterruptedException{
			try{
			String[] tokens = key.toString().split("\\s+");
			String country = tokens[0];
			int age = Integer.parseInt(tokens[1]);
			String name = tokens[2];
			int height = Integer.parseInt(tokens[3]);
			
			outputKey.setString("country",country);
			outputKey.setInt("age",age);
			outputKey.setString("name",name);
			outputKey.setInt("height", height);
			emit(outputKey);
			} catch(InvalidFieldException e){
				throw new RuntimeException(e);
			}
		}


		
	}
	
	private static class Red extends GrouperReducerHandler<Text,NullWritable>{

		@Override
    public void onOpenGroup(int depth,String field,Tuple firstElement) throws IOException,InterruptedException{
			Text text = new Text("OPEN("+ depth+","+field +"):\t\t" + firstElement);
	    emit(text,NullWritable.get());
    }

		@Override
    public void onCloseGroup(int depth,String field,Tuple lastElement) throws IOException,InterruptedException {
	    Text text = new Text("CLOSE:("+depth+","+field+")\t\t" + lastElement);
			emit(text,NullWritable.get());
    }
		
		@Override
		public void onGroupElements(Iterable<Tuple> tuples) throws IOException,InterruptedException {
			Iterator<Tuple> iterator = tuples.iterator();
			Text text = new Text();
			while ( iterator.hasNext()){
				Tuple tuple = iterator.next();
				text.set("element:\t\t" + tuple);
				emit(text,NullWritable.get());
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
		grouper.setMapperHandler(Mapy.class);
		grouper.setReducerHandler(Red.class);
		
		grouper.setSchema(FieldsDescription.parse("country:string , age:vint , name:string,height:int"));
		grouper.setSortCriteria("country ASC,age ASC");
		grouper.setMinGroup("country");
		grouper.setMaxGroup("country,age,name");
		
		grouper.setOutputKeyClass(TupleImpl.class);
		grouper.setOutputValueClass(NullWritable.class);
		
		
		Job job = grouper.getJob();
		job.setNumReduceTasks(2);
//		MultipleInputs.addInputPath(job, new Path("input*"),SequenceFileInputFormat.class, Mapy.class);
//		MultipleInputs.addInputPath(job, new Path("input"),SequenceFileInputFormat.class, Mapy.class);
//		
		System.out.println("Formats:"+job.getConfiguration().get("mapred.input.dir.formats"));
		System.out.println("Mappers:"+job.getConfiguration().get("mapred.input.dir.mappers"));
		
		
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		assertRun(job);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, GrouperException{
		Test test = new Test();
		test.initHadoop();
		test.test();
	}
	
}
