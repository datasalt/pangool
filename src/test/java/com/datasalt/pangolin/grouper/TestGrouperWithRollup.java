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
import com.datasalt.pangolin.grouper.mapred.GrouperWithRollupReducer;


public class TestGrouperWithRollup extends AbstractHadoopTestLibrary{

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
			try {
				outputKey.setField("country",country);
				outputKey.setField("age",age);
				outputKey.setField("name",name);
				outputKey.setField("height", height);
				emit(outputKey);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
			
		}
	}
	
	private static class Red extends GrouperWithRollupReducer<Tuple,NullWritable>{

		@Override
    public void onOpenGroup(int depth,String field,Tuple firstElement, Context context) {
	    System.out.println("OPEN("+ depth+","+field +"):\t\t" + firstElement);
    }

		@Override
    public void onCloseGroup(int depth,String field,Tuple lastElement, Context context) {
	    System.out.println("CLOSE:("+depth+","+field+")\t\t" + lastElement);
    }
		
		@Override
		public void onElements(Iterable<Tuple> tuples, Context context) throws IOException,InterruptedException {
			Iterator<Tuple> iterator = tuples.iterator();
			while ( iterator.hasNext()){
				Tuple tuple = iterator.next();
				System.out.println("element:\t\t" + tuple);
			}
	  }
	}
	
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException, GrouperException{
		
		withInput("input",writable("ES 20 listo 250"));
		withInput("input",writable("US 14 perro 180"));
		withInput("input",writable("US 14 perro 170"));
		withInput("input",writable("US 14 beber 202"));
		withInput("input",writable("US 15 jauja 160"));
		withInput("input",writable("US 16 listo 160"));
		withInput("input",writable("XE 20 listo 230"));
		
		GrouperWithRollup grouper = new GrouperWithRollup(getConf());
		grouper.setInputFormat(SequenceFileInputFormat.class);
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setMapperClass(Mapy.class);
		grouper.setReducerClass(Red.class);
		
		grouper.setSchema(Schema.parse("country:string,age:vint,name:string,height:int"));
		grouper.setSortCriteria("country DESC,age DESC");
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
