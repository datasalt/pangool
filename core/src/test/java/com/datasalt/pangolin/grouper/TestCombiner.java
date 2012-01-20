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
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.io.tuple.DoubleBufferPangolinTuple;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.grouper.mapreduce.handler.GroupHandler;
import com.datasalt.pangool.test.AbstractHadoopTestLibrary;

@SuppressWarnings({"unchecked"})
public class TestCombiner extends AbstractHadoopTestLibrary{

	private static class Mapy extends InputProcessor<Text,NullWritable>{
		
		private DoubleBufferPangolinTuple outputTuple;
		
    @Override
		public void setup(Schema schema,Context context) throws IOException,InterruptedException {
			//super.setup(schema,context);
			this.outputTuple = new DoubleBufferPangolinTuple();
			
		}
		
    @Override
		public void process(Text key,NullWritable value,Collector collector) throws IOException,InterruptedException{
			String[] tokens = key.toString().split("\\s+");
			String country = tokens[0];
			int age = Integer.parseInt(tokens[1]);
			String name = tokens[2];
			int height = Integer.parseInt(tokens[3]);
				outputTuple.setString("country",country);
				outputTuple.setInt("age", age);
				outputTuple.setString("name", name);
				outputTuple.setInt("height", height);
				collector.write(outputTuple);
//			} catch (InvalidFieldException e) {
//				throw new RuntimeException(e);
//			}
		}
		
	}
	
	private static class Red extends GroupHandler<ITuple,NullWritable>{

		
		
		@Override
    public void onOpenGroup(int depth,String field,ITuple firstElement,Reducer.Context context) {
			
	    System.out.println("OPEN("+ depth+","+field +"):\t\t" + firstElement);
    }

		@Override
    public void onCloseGroup(int depth,String field,ITuple lastElement,Reducer.Context context) {
	    System.out.println("CLOSE:("+depth+","+field+")\t\t" + lastElement);
    }
		
		@Override
		public void onGroupElements(Iterable<ITuple> tuples,Context context) throws IOException,InterruptedException {
			
			Iterator<ITuple> iterator = tuples.iterator();
			if (iterator.hasNext()){
			while ( iterator.hasNext()){
				ITuple tuple = iterator.next();
				System.out.println("element:\t\t" + tuple);
			}
			}
	  
	}}
	
	private static class Combi extends GroupHandler<ITuple,NullWritable>{

		//private int numFields;
		
		
    public void setup(Schema schema,Reducer.Context context) throws IOException,InterruptedException,GrouperException {
			
		}
		
		@Override
    public void onOpenGroup(int depth,String field,ITuple firstElement,Context context) throws IOException,InterruptedException,GrouperException  {
			
	    System.out.println("COMBI OPEN("+ depth+","+field +"):\t\t" + firstElement);
	    //emit(firstElement);
		}

		@Override
    public void onCloseGroup(int depth,String field,ITuple lastElement,Context context) throws IOException,InterruptedException,GrouperException {
	    System.out.println("COMBI CLOSE:("+depth+","+field+")\t\t" + lastElement);
    }

		

		@Override
    public void onGroupElements(Iterable<ITuple> tuples, Reducer.Context context) throws IOException, InterruptedException,GrouperException  {
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
		
		SchemaBuilder schemaBuilder = new SchemaBuilder();
		schemaBuilder.addField("country", String.class);
		schemaBuilder.addField("age", VIntWritable.class);
		schemaBuilder.addField("name", String.class);
		schemaBuilder.addField("height", Integer.class);
		
		SortCriteriaBuilder sortBuilder = new SortCriteriaBuilder();
		sortBuilder.addSortElement("country", SortOrder.ASC);
		sortBuilder.addSortElement("age",SortOrder.ASC);
		sortBuilder.addSortElement("name",SortOrder.ASC);
		SortCriteria sortCriteria = sortBuilder.createSortCriteria();
		//SortCriteria sortCriteria = SortCriteria.parse("country ASC,age ASC");
		
		Grouper grouper = new Grouper(getConf());
		
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setOutputHandler(Red.class);
		grouper.setSchema(schemaBuilder.createSchema());
		grouper.setSortCriteria(sortCriteria);
		grouper.setRollupBaseFieldsToGroupBy("country");
		grouper.setFieldsToGroupBy("country","age","name");
		grouper.setCombinerHandler(Combi.class);
		grouper.setOutputKeyClass(DoubleBufferPangolinTuple.class);
		grouper.setOutputValueClass(NullWritable.class);
		grouper.addInput(new Path("input"),SequenceFileInputFormat.class, Mapy.class);
		grouper.setOutputPath(new Path("output"));
		Job job = grouper.createJob();
		
		assertRun(job);
	}
}
