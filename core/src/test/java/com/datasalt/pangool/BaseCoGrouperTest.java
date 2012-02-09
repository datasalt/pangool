package com.datasalt.pangool;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datasalt.pangool.api.GroupHandler;
import com.datasalt.pangool.api.GroupHandlerWithRollup;
import com.datasalt.pangool.api.InputProcessor;

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

	
	
	
	@BeforeClass
	public static void files() throws IOException {
		new File("input").createNewFile();
	}
	
	@AfterClass
	public static void deleteFiles() {
		new File("input").delete();
	}
}
