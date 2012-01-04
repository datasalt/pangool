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

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.TupleFactory;
import com.datasalt.pangolin.grouper.io.tuple.GroupComparator;
import com.datasalt.pangolin.grouper.io.tuple.Partitioner;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapreduce.Mapper;
import com.datasalt.pangolin.grouper.mapreduce.handler.ReducerHandler;


public class TestMapRedCounter extends AbstractHadoopTestLibrary{

	private static class Mapy extends Mapper<Text,NullWritable>{
		
		private FieldsDescription schema;
		
		@SuppressWarnings({ "unchecked" })
    @Override
		public void setup(FieldsDescription schema,Context context) throws IOException,InterruptedException {
			this.schema = schema;
			
		}
		
		
		@SuppressWarnings("unchecked")
		@Override
		public void map(Text key,NullWritable value,Collector collector) throws IOException,InterruptedException{
			try {
				Tuple outputKey = createTuple(key.toString(), schema);
				collector.write(outputKey);
			} catch (InvalidFieldException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private static class IdentityRed extends ReducerHandler<Text,Text>{

		private Reducer.Context context;
		private int [] count,distinctCount;
		private int minDepth;
		private int maxDepth;
		
		@Override
		public void setup(FieldsDescription schema,Reducer.Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();	
			
			String[] baseGroup = Partitioner.getPartitionerFields(conf);
			String[] maxGroup = GroupComparator.getGroupComparatorFields(conf);
			minDepth = baseGroup.length - 1;
			maxDepth = maxGroup.length - 1;
			this.context = context;
			count = new int[maxDepth + 1];
			distinctCount = new int[maxDepth + 1];
		}
		
		@Override
		public void cleanup(FieldsDescription schema,Reducer.Context context) throws IOException,InterruptedException {
			
		}
		
		@Override
    public void onOpenGroup(int depth,String field,ITuple firstElement,Reducer.Context context) throws IOException, InterruptedException {
			count[depth] = 0;
			distinctCount[depth]=0;
			
    }

		@Override
    public void onCloseGroup(int depth,String field,ITuple lastElement,Reducer.Context context) throws IOException, InterruptedException {
			try {
				String tupleStr = lastElement.toString(0, depth);
				String output =  tupleStr +  " => count:" + count[depth];
				if (depth < maxDepth){
					//distinctCount is not set in highest depth
					output += " distinctCount:"+ distinctCount[depth];
				}
				System.out.println(output);
				if(depth > minDepth) {
					//we can't output data below minDepth.
					count[depth - 1] += count[depth];
					distinctCount[depth - 1]++;
				}
			} catch(InvalidFieldException e) {
				throw new RuntimeException(e);
			}
    }
		
		@Override
		public void onGroupElements(Iterable<ITuple> tuples,Reducer.Context context) throws IOException,InterruptedException {
			Iterator<ITuple> iterator = tuples.iterator();

			try {
				while(iterator.hasNext()) {
					ITuple tuple = iterator.next();
					count[maxDepth] += tuple.getInt("count");
				}
			} catch(InvalidFieldException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	
	private static Tuple createTuple(String text,FieldsDescription schema) throws InvalidFieldException{
		Tuple tuple = TupleFactory.createTuple(schema);
		String[] tokens = text.split(",");
		String user = tokens[0];
		Integer day = Integer.parseInt(tokens[1]);
		String url = tokens[2];
		
		tuple.setString("user",user);
		tuple.setInt("day",day);
		tuple.setString("url",url);
		tuple.setInt("count", 1);
		return tuple;
	}
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException, GrouperException, InstantiationException, IllegalAccessException{
		
		String[] inputElements = new String[]{
				"user1,1,url1",
				"user1,1,url1",
				"user1,1,url1",
				"user1,1,url2",
				"user1,1,url2",
				"user1,1,url2",
				"user1,1,url3",
				"user1,2,url1",
				"user1,2,url1",
				"user1,2,url1",
				"user1,2,url2",
				"user1,3,url4",
				"user1,3,url4",
				"user1,3,url5",
				"user2,1,url6",
				"user2,1,url6",
				"user2,1,url7",
				"user2,2,url8"
		};
		
		FieldsDescription schema = FieldsDescription.parse("user:string,day:vint,url:string,count:vint");
		
		for (String inputElement : inputElements){
			withInput("input",writable(inputElement));
		}
		
		Grouper grouper = new Grouper(getConf());
		grouper.setInputFormat(SequenceFileInputFormat.class);
		grouper.setOutputFormat(SequenceFileOutputFormat.class);
		grouper.setMapper(Mapy.class);
		grouper.setReducerHandler(IdentityRed.class);
		
		grouper.setSchema(schema);
		SortCriteria sortCriteria = SortCriteria.parse("user ASC,day ASC,url ASC");
		grouper.setSortCriteria(sortCriteria);
		grouper.setRollupBaseGroupFields("user");
		grouper.setGroupFields("user","day","url");
		
		grouper.setOutputKeyClass(Text.class);
		grouper.setOutputValueClass(Text.class);
		
		
		Job job = grouper.createJob();
		job.setNumReduceTasks(1);
		
		Path inputPath = new Path("input");
		Path outputPath = new Path("output");
		FileInputFormat.setInputPaths(job,inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		assertRun(job);
		
	}
	
	private void assertOutput(SequenceFile.Reader reader,String expectedKey,ITuple expectedValue) throws IOException{
		Text actualKey=new Text();
		Text actualValue = new Text();
		reader.next(actualKey, actualValue);
		
		Assert.assertEquals(new Text(expectedKey),actualKey);
		Assert.assertEquals(new Text(expectedValue.toString()),actualValue);
	}
}

