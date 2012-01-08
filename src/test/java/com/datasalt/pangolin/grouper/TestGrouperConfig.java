package com.datasalt.pangolin.grouper;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.grouper.mapreduce.handler.GroupHandler;

/**
 * This test pretends to check if {@link Grouper} complains correctly about incoherence about {@link SortCriteria}, {@link Schema} and fields to be groupped.
 * @author eric
 *
 */
public class TestGrouperConfig extends BaseTest{
	
	private void checkException(Grouper grouper) throws IOException{
		try{
		Job job = grouper.createJob();
		Assert.fail();
		} catch(GrouperException e){
			System.out.println(e);
		}
	}
	
	
	private static class Mapper extends InputProcessor {

		@Override
    public void setup(Schema schema, Context context) throws IOException, InterruptedException, GrouperException {
	    // TODO Auto-generated method stub
	    
    }

		@Override
    public void process(Object key, Object value, Collector collector) throws IOException, InterruptedException,
        GrouperException {
	    // TODO Auto-generated method stub
	    
    }
		
	}
	
	private static class Reducer extends GroupHandler  {

		@Override
    public void onOpenGroup(int depth, String field, ITuple firstElement, Context context) throws IOException,
        InterruptedException, GrouperException {
	    // TODO Auto-generated method stub
	    
    }

		@Override
    public void onCloseGroup(int depth, String field, ITuple lastElement, Context context) throws IOException,
        InterruptedException, GrouperException {
	    // TODO Auto-generated method stub
	    
    }

		@Override
    public void onGroupElements(Iterable tuples, Context context) throws IOException, InterruptedException,
        GrouperException {
	    // TODO Auto-generated method stub
	    
    }
		
	}
	
	@Test
	public void test() throws IOException, GrouperException{
		
		Schema schema = Schema.parse("country:string, age:vint, name:string, height:long");
		SortCriteria sortCriteria = SortCriteria.parse("country DESC,age ASC,name asc,height desc");
		Grouper grouper = new Grouper(getConf());
		checkException(grouper);	
		grouper.setSchema(schema);
		checkException(grouper);
		grouper.setSortCriteria(sortCriteria);
		checkException(grouper);
		grouper.setOutputHandler(Reducer.class);
		checkException(grouper);
		grouper.addInput(new Path("input"), TextInputFormat.class, Mapper.class);
		checkException(grouper);
		grouper.setFieldsToGroupBy("country","age");
		checkException(grouper);
		grouper.setOutputFormat(TextOutputFormat.class);
		checkException(grouper);
		grouper.setOutputKeyClass(Text.class);
		checkException(grouper);
		grouper.setOutputValueClass(Text.class);
		checkException(grouper);
		grouper.setOutputPath(new Path("output"));
		
		Job job = grouper.createJob();
		
		
		grouper.setFieldsToGroupBy("age"); 
		checkException(grouper);
		grouper.setFieldsToGroupBy("");
		checkException(grouper);
		grouper.setFieldsToGroupBy("country","age","name","height","blabla");
		checkException(grouper);
		
		grouper.setFieldsToGroupBy("country","age");
		grouper.setRollupBaseFieldsToGroupBy("country");
		job = grouper.createJob();
		grouper.setRollupBaseFieldsToGroupBy("age");
		checkException(grouper);
		grouper.setRollupBaseFieldsToGroupBy("country","age","name");
		checkException(grouper);
		
		//TODO
		
	}

}
