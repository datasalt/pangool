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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.DoubleBufferPangolinTuple;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.grouper.mapreduce.handler.GroupHandler;
import com.datasalt.pangool.test.AbstractHadoopTestLibrary;


public class TestMultipleInputs extends AbstractHadoopTestLibrary{

	private static class Mapy1 extends InputProcessor<Text,NullWritable>{
		
		private DoubleBufferPangolinTuple outputTuple;
		
		@Override
		public void setup(Schema schema, Context context) throws IOException,InterruptedException {
			this.outputTuple = new DoubleBufferPangolinTuple();
		}
		
		
		@Override
		public void process(Text key,NullWritable value,Collector collector) throws IOException,InterruptedException,GrouperException {
			String[] tokens = key.toString().split("\\s+");
			Integer id = Integer.parseInt(tokens[0]);
			String country = tokens[1];
			Integer age = Integer.parseInt(tokens[2]);
			String name = tokens[3];
			Integer height = Integer.parseInt(tokens[4]);
			outputTuple.setInt("id",id);
			outputTuple.setString("country", country);
			outputTuple.setInt("age", age);
			outputTuple.setString("name", name);
			outputTuple.setLong("height", height);
			collector.write(outputTuple);
		}
	}
	
	private static class Mapy2 extends InputProcessor<Text,NullWritable>{
		
		private DoubleBufferPangolinTuple outputTuple;
		
		@Override
		public void setup(Schema schema, Context context) throws IOException,InterruptedException {
			this.outputTuple = new DoubleBufferPangolinTuple();
		}
		
		
		@Override
		public void process(Text key,NullWritable value,Collector collector) throws IOException,InterruptedException,GrouperException {
			String[] tokens = key.toString().split("\\s+");
			Integer id = Integer.parseInt(tokens[0]);
			String surname  = tokens[1];
			outputTuple.setString("surname", surname);
			outputTuple.setInt("id", id);
			outputTuple.setInt("age", 0);
			outputTuple.setString("name", "");
			outputTuple.setLong("height", 456);
			collector.write(outputTuple);
			
		}
	}
	
	private static class Red extends GroupHandler<ITuple,NullWritable>{

		@Override
		public void setup(Schema schema,Reducer.Context context) throws IOException,InterruptedException {
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
		
		withInput("input1",new Text("1 ES 20 eric 250"));
		withInput("input1",new Text("2 US 14 ivan 180"));
		withInput("input1",new Text("3 US 14 pere 170"));
		withInput("input1",new Text("4 US 14 bieber 202"));
		withInput("input1",new Text("5 US 15 jauja 160"));
		withInput("input1",new Text("6 US 16 listo 160"));
		withInput("input1",new Text("7 XE 20 listo 230"));
		
		withInput("input2", new Text("1 palacios"));
		withInput("input2", new Text("2 deprado"));
		withInput("input3", new Text("3 ferrera"));
		
		
		Schema schema = Schema.parse("id:vint, country:string, age:vint, name:string, height:long, surname:string");
		SortCriteria sortCriteria = SortCriteria.parse("id ASC");
		
		Grouper grouper = new Grouper(getConf());
		grouper.setJarByClass(TestSimpleGrouper.class);
		grouper.setSchema(schema);
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setOutputHandler(Red.class);
		
		grouper.setSortCriteria(sortCriteria);
		grouper.setFieldsToGroupBy("id");
		
		grouper.setOutputKeyClass(DoubleBufferPangolinTuple.class);
		grouper.setOutputValueClass(NullWritable.class);
		grouper.addInput(new Path("input1"), SequenceFileInputFormat.class, Mapy1.class);
		grouper.addInput(new Path("input2"), SequenceFileInputFormat.class, Mapy2.class);
		grouper.setOutputPath(new Path("output"));
		Job job = grouper.createJob();
		
		assertRun(job);
		//TODO check output
	}
}

