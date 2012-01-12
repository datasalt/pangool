package com.datasalt.pangolin.pangool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;

public class TestCoGrouperBasicChecks extends BaseCoGrouperTest {

	@Test(expected=CoGrouperException.class)
	public void testMissingSchemas() throws CoGrouperException, InvalidFieldException, IOException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.setSorting(getTestSorting())
			.groupBy("date", "url")
			.addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
			.setGroupHandler(myGroupHandler.getClass())
			.setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
		
		grouper.createJob();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testMissingSorting() throws CoGrouperException, IOException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.addSchema(1, "url:string, date:long, content:string")
			.addSchema(2, "url:string, date:long, name:string, surname:string")
			.groupBy("date", "url")
			.addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
			.setGroupHandler(myGroupHandler.getClass())
			.setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
		
		grouper.createJob();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testMissingGroupBy() throws CoGrouperException, InvalidFieldException, IOException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.addSchema(1, "url:string, date:long, content:string")
			.addSchema(2, "url:string, date:long, name:string, surname:string")
			.setSorting(getTestSorting())
			.addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
			.setGroupHandler(myGroupHandler.getClass())
			.setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
		
		grouper.createJob();
	}

	@Test(expected=CoGrouperException.class)
	public void testMissingInputs() throws CoGrouperException, InvalidFieldException, IOException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.addSchema(1, "url:string, date:long, content:string")
			.addSchema(2, "url:string, date:long, name:string, surname:string")
			.groupBy("date", "url")
			.setSorting(getTestSorting())
			.setGroupHandler(myGroupHandler.getClass())
			.setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
		
		grouper.createJob();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testMissingOutput() throws CoGrouperException, InvalidFieldException, IOException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.addSchema(1, "url:string, date:long, content:string")
			.addSchema(2, "url:string, date:long, name:string, surname:string")
			.groupBy("date", "url")
			.addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
			.setSorting(getTestSorting())
			.setGroupHandler(myGroupHandler.getClass());
		
		grouper.createJob();
	}
	
	@Test(expected=CoGrouperException.class)
	public void testMissingGroupHandler() throws CoGrouperException, InvalidFieldException, IOException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.addSchema(1, "url:string, date:long, content:string")
			.addSchema(2, "url:string, date:long, name:string, surname:string")
			.groupBy("date", "url")
			.addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
			.setSorting(getTestSorting())
			.setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
		
		grouper.createJob();
	}

	@Test
	public void testAllFine() throws CoGrouperException, InvalidFieldException, IOException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.addSchema(1, "url:string, date:long, content:string")
			.addSchema(2, "url:string, date:long, name:string, surname:string")
			.groupBy("url", "date")
			.addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
			.setSorting(getTestSorting())
			.setGroupHandler(myGroupHandler.getClass())
			.setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
		
		grouper.createJob();
	}
}
