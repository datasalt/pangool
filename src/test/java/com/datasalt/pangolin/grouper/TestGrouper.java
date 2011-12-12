/**
 * Copyright [2011] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.datasalt.pangolin.grouper;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleImpl;
import com.datasalt.pangolin.grouper.io.TupleImpl.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapred.GrouperMapper;
import com.datasalt.pangolin.grouper.mapred.GrouperMapperHandler;
import com.datasalt.pangolin.grouper.mapred.GrouperReducerHandler;
import com.datasalt.pangolin.grouper.mapred.SimpleGrouperReducer;


public class TestGrouper extends AbstractHadoopTestLibrary{

	private static class Mapy extends GrouperMapperHandler<Text,NullWritable>{
		
		private TupleImpl outputKey;
		
		@Override
		public void setup(Mapper.Context context) throws IOException,InterruptedException {
			super.setup(context);
		}
		
		
		@Override
		public void map(Text key,NullWritable value) throws IOException,InterruptedException{
			String[] tokens = key.toString().split("\\s+");
			String country = tokens[0];
			Integer age = Integer.parseInt(tokens[1]);
			String name = tokens[2];
			Integer height = Integer.parseInt(tokens[3]);
			
			try{
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
	
	private static class Red extends GrouperReducerHandler<Tuple,NullWritable>{

		@Override
    public void onOpenGroup(int depth, String field, Tuple firstElement) throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    
    }

		@Override
    public void onCloseGroup(int depth, String field, Tuple lastElement) throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    
    }

		@Override
    public void onGroupElements(Iterable<Tuple> values) throws IOException, InterruptedException {
			StringBuilder b = new StringBuilder();
		for (Tuple value : values){
			b.append(value.toString()).append(",");
		}
		System.out.println("ELEMENTS:"+b.toString());
	
	    
    }

//		@Override
//		public void onGroupElements(Iterable<Tuple> values) throws IOException, InterruptedException {
//			StringBuilder b = new StringBuilder();
//			for (Tuple value : values){
//				b.append(value.toString()).append(",");
//			}
//			System.out.println("ELEMENTS:"+b.toString());
//		}
	}
	
	
//	@Test
//	public void test() throws IOException, InterruptedException, ClassNotFoundException, GrouperException{
//		
//		withInput("input",new Text("ES 20 listo 250"));
//		withInput("input",new Text("US 14 perro 180"));
//		withInput("input",new Text("US 14 perro 170"));
//		withInput("input",new Text("US 14 beber 202"));
//		withInput("input",new Text("US 15 jauja 160"));
//		withInput("input",new Text("US 16 listo 160"));
//		withInput("input",new Text("XE 20 listo 230"));
//		
//		Grouper grouper = new Grouper(getConf());
//		grouper.setInput(0,new Path("input"),SequenceFileInputFormat.class,Mapy.class, "country:string,age:vint,name:string,height:int");
//		grouper.setOutputFormat(SequenceFileOutputFormat.class);
//		grouper.setReducerClass(Red.class);
//		
//		grouper.setSortCriteria("country DESC,age ASC,name asc,height desc");
//		grouper.setGroup("country,age");
//		
//		grouper.setOutputKeyClass(Tuple.class);
//		grouper.setOutputValueClass(NullWritable.class);
//		
//		
//		Job job = grouper.getJob();
//		FileInputFormat.setInputPaths(job,new Path("input"));
//		FileOutputFormat.setOutputPath(job, new Path("output"));
//		
//		assertRun(job);
//	}
}
