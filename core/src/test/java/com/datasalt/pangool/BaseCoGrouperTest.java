package com.datasalt.pangool;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datasalt.pangool.SortCriteria.SortOrder;
import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.GroupHandlerWithRollup;
import com.datasalt.pangool.api.InputProcessor;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;

@SuppressWarnings("rawtypes")
public abstract class BaseCoGrouperTest {

	protected static GroupHandler myGroupHandler = new GroupHandler();
	protected static GroupHandler myGroupHandlerWithRollup = new GroupHandlerWithRollup();
	protected static InputProcessor myInputProcessor = new InputProcessor() {

		/**
     * 
     */
    private static final long serialVersionUID = 1L;

		@Override
		public void process(Object key, Object value, CoGrouperContext context, Collector collector) throws IOException,
		    InterruptedException {

		}
	};

	protected Sorting getTestSorting() throws InvalidFieldException, CoGrouperException {

		return new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.DESC)
		  .secondarySort(1)
		  	.add("content", SortOrder.ASC)
		  .secondarySort(2)
		   	.add("name", SortOrder.ASC)
		  .buildSorting();
	}
	
	
	@BeforeClass
	public static void files() throws IOException {
		new File("input").createNewFile();
	}
	
	@AfterClass
	public static void deleteFiles() {
		new File("input").delete();
	}
}
