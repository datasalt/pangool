package com.datasalt.pangool.integration;

import java.io.IOException;

import org.junit.Test;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.io.tuple.BaseTuple;
import com.datasalt.pangool.mapreduce.InputProcessor;

public class TestMultipleSchemas {

	public static class FirstInputProcessor extends InputProcessor {
		
		@Override
		public void process(Object key, Object value, Collector collector) throws IOException, InterruptedException,
		    GrouperException {
			
			BaseTuple tuple = new BaseTuple();
			
		}
	}
	
	@Test
	public void test() {
		
	}
}
