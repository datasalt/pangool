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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.TupleFactory;
import com.datasalt.pangolin.grouper.mapreduce.Mapper;
import com.datasalt.pangolin.grouper.mapreduce.handler.ReducerHandler;


public class TestSimpleGrouper extends AbstractHadoopTestLibrary{

	private static class Mapy extends Mapper<Text,NullWritable>{
		
		private Tuple outputTuple;
		
		@Override
		@SuppressWarnings("rawtypes")
		public void setup(FieldsDescription schema, Context context) throws IOException,InterruptedException {
			this.outputTuple = TupleFactory.createTuple(schema);
		}
		
		
		@Override
		public void map(Text key,NullWritable value,Collector collector) throws IOException,InterruptedException{
			String[] tokens = key.toString().split("\\s+");
			String country = tokens[0];
			Integer age = Integer.parseInt(tokens[1]);
			String name = tokens[2];
			Integer height = Integer.parseInt(tokens[3]);
			
			try {
				outputTuple.setString("country", country);
				outputTuple.setInt("age", age);
				outputTuple.setString("name", name);
				outputTuple.setLong("height", height);
				collector.write(outputTuple);
			} catch(InvalidFieldException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private static class Red extends ReducerHandler<ITuple,NullWritable>{

		@Override
		public void setup(FieldsDescription schema,Reducer.Context context) throws IOException,InterruptedException {
			System.out.println("REd setup");
		}
		
		@Override
    public void onOpenGroup(int depth, String field, ITuple firstElement,Reducer.Context context) throws IOException, InterruptedException {
	    
    }

		@Override
    public void onCloseGroup(int depth, String field, ITuple lastElement,Reducer.Context context) throws IOException, InterruptedException {
	   
	    
    }

		@Override
    public void onGroupElements(Iterable<ITuple> values,Reducer.Context context) throws IOException, InterruptedException {
			StringBuilder b = new StringBuilder();
			for(ITuple value : values) {
				b.append(value.toString()).append(",");
			}
			System.out.println("ELEMENTS:" + b.toString());
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
		grouper.setJarByClass(TestSimpleGrouper.class);
		FieldsDescription schema = FieldsDescription.parse("country:string, age:vint, name:string, height:long");
		grouper.setSchema(schema);
		//grouper.setInputFormat(SequenceFileInputFormat.class);
		//grouper.setMapper(Mapy.class);
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setReducerHandler(Red.class);
		
		SortCriteria sortCriteria = SortCriteria.parse("country DESC,age ASC,name asc,height desc");
		grouper.setSortCriteria(sortCriteria);
		grouper.setGroupFields("country","age");
		//grouper.setRollupBaseGroupFields("country");
		
		grouper.setOutputKeyClass(Tuple.class);
		grouper.setOutputValueClass(NullWritable.class);
		
		
		Job job = grouper.createJob();
		System.out.println(job.getReducerClass());
		MultipleInputs.addInputPath(job, new Path("input"), SequenceFileInputFormat.class,Mapy.class);
		//FileInputFormat.setInputPaths(job,new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		assertRun(job);
		
		//TODO check output
	}
}
