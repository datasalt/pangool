package com.datasalt.pangolin.pangool;

import java.io.IOException;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.mapreduce.InputProcessor;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;
import com.datasalt.pangolin.pangool.mapreduce.GroupHandler;
import com.datasalt.pangolin.pangool.mapreduce.GroupHandlerWithRollup;

@SuppressWarnings("rawtypes")
public class BaseCoGrouperTest {

	protected GroupHandler myGroupHandler = new GroupHandler() {

		@Override
    public void onGroupElements(Iterable tuples, org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException, CoGrouperException {
	    
    }
	};
	
	protected GroupHandler myGroupHandlerWithRollup = new GroupHandlerWithRollup() {

		@Override
    public void onOpenGroup(int depth, String field, ITuple firstElement,
        org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException,
        CoGrouperException {
	    
    }

		@Override
    public void onCloseGroup(int depth, String field, ITuple lastElement,
        org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException,
        CoGrouperException {
	    
    }

		@Override
    public void onGroupElements(Iterable tuples, org.apache.hadoop.mapreduce.Reducer.Context context)
        throws IOException, InterruptedException, CoGrouperException {
	    
    }
	};

	protected InputProcessor myInputProcessor = new InputProcessor() {

		@Override
		public void setup(Schema schema, Context context) throws IOException, InterruptedException, GrouperException {

		}

		@Override
		public void process(Object key, Object value, Collector collector) throws IOException, InterruptedException,
		    GrouperException {

		}
	};
	
	protected Sorting getTestSorting() throws InvalidFieldException {
		return new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.DESC)
			.secondarySort(1).add("content", SortOrder.ASC)
			.secondarySort(2).add("name", SortOrder.ASC)
			.buildSorting();
	}
}
