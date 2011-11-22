package com.datasalt.pangolin.grouper;

import java.io.IOException;

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
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.Tuple.NoSuchFieldException;
import com.datasalt.pangolin.grouper.mapred.GrouperMapper;
import com.datasalt.pangolin.grouper.mapred.SimpleGrouperReducer;


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
			try{
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
	
	private static class Red extends SimpleGrouperReducer<Tuple,NullWritable>{

		@Override
		public void elements(Iterable<Tuple> values,Context context)
				throws IOException, InterruptedException {
			StringBuilder b = new StringBuilder();
			for (Tuple value : values){
				b.append(value.toString()).append(",");
			}
			System.out.println("ELEMENTS:"+b.toString());
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
		grouper.setSortCriteria("country DESC,age ASC,name asc,height desc");
		grouper.setGroup("country,age");
		
		grouper.setOutputKeyClass(Tuple.class);
		grouper.setOutputValueClass(NullWritable.class);
		
		
		Job job = grouper.getJob();
		FileInputFormat.setInputPaths(job,new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		assertRun(job);
	}
}
