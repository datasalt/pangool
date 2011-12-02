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
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.Tuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapred.GrouperMapperHandler;
import com.datasalt.pangolin.grouper.mapred.GrouperReducerHandler;
import com.datasalt.pangolin.grouper.mapred.GrouperWithRollupCombiner;
import com.datasalt.pangolin.grouper.mapred.GrouperMapper;
import com.datasalt.pangolin.grouper.mapred.GrouperWithRollupReducer;


public class TestCombiner extends AbstractHadoopTestLibrary{

	private static class Mapy extends GrouperMapperHandler<Text,NullWritable>{
		
		private Tuple outputKey;
		@Override
		public void setup(Mapper.Context context) throws IOException,InterruptedException {
			super.setup(context);
			try{
			FieldsDescription schema = FieldsDescription.parse(context.getConfiguration());
			} catch(GrouperException e){
				throw new RuntimeException (e);
			}
		}
		
		
		@Override
		public void map(Text key,NullWritable value) throws IOException,InterruptedException{
			String[] tokens = key.toString().split("\\s+");
			String country = tokens[0];
			int age = Integer.parseInt(tokens[1]);
			String name = tokens[2];
			int height = Integer.parseInt(tokens[3]);
			
			
			try {
				outputKey.setString("country",country);
				outputKey.setInt("age", age);
				outputKey.setString("name", name);
				outputKey.setInt("height", height);
			emit(outputKey);
			} catch (InvalidFieldException e) {
				throw new RuntimeException(e);
			}
		}
		
	}
	
	private static class Red extends GrouperReducerHandler<Tuple,NullWritable>{

		
		
		@Override
    public void onOpenGroup(int depth,String field,Tuple firstElement) {
			
	    System.out.println("OPEN("+ depth+","+field +"):\t\t" + firstElement);
    }

		@Override
    public void onCloseGroup(int depth,String field,Tuple lastElement) {
	    System.out.println("CLOSE:("+depth+","+field+")\t\t" + lastElement);
    }
		
		@Override
		public void onGroupElements(Iterable<Tuple> tuples) throws IOException,InterruptedException {
			
			Iterator<Tuple> iterator = tuples.iterator();
			if (iterator.hasNext()){
			while ( iterator.hasNext()){
				Tuple tuple = iterator.next();
				System.out.println("element:\t\t" + tuple);
			}
			}
	  
	}}
	
	private static class Combi extends GrouperReducerHandler<Tuple,NullWritable>{

		private int numFields;
		
		public void setup(Reducer.Context context) throws IOException,InterruptedException {
			super.setup(context);
			//numFields = getSchema().getFields().length;
			
		}
		
		@Override
    public void onOpenGroup(int depth,String field,Tuple firstElement) throws IOException,InterruptedException  {
			
	    System.out.println("COMBI OPEN("+ depth+","+field +"):\t\t" + firstElement);
	    //emit(firstElement);
		}

		@Override
    public void onCloseGroup(int depth,String field,Tuple lastElement) {
	    System.out.println("COMBI CLOSE:("+depth+","+field+")\t\t" + lastElement);
    }

		@Override
    public void onGroupElements(Iterable<Tuple> tuples) throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    
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
		
		GrouperWithRollup grouper = new GrouperWithRollup(getConf());
		grouper.setInputFormat(SequenceFileInputFormat.class);
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setMapperHandler(Mapy.class);
		grouper.setReducerHandler(Red.class);
		
		grouper.setSchema(FieldsDescription.parse("country:string , age:vint , name:string,height:int"));
		grouper.setSortCriteria("country ASC,age ASC");
		grouper.setMinGroup("country");
		grouper.setMaxGroup("country,age,name");
		grouper.setCombinerHandler(Combi.class);
		grouper.setOutputKeyClass(Tuple.class);
		grouper.setOutputValueClass(NullWritable.class);
		
		
		Job job = grouper.getJob();
		FileInputFormat.setInputPaths(job,new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		assertRun(job);
	}
}
