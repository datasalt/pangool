//package com.datasalt.avrool;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.junit.Test;
//
//import com.datasalt.avrool.CoGrouper;
//import com.datasalt.avrool.CoGrouperConfig;
//import com.datasalt.avrool.CoGrouperConfigBuilder;
//import com.datasalt.avrool.CoGrouperException;
//import com.datasalt.avrool.PangoolSchema;
//import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;
//
//public class TestCoGrouperBasicChecks extends BaseCoGrouperTest {
//
//	@Test(expected = CoGrouperException.class)
//	public void testMissingSchemas() throws CoGrouperException, InvalidFieldException, IOException {
//		new CoGrouperConfigBuilder().setGroupByFields("url", "date").setSorting(getTestSorting()).build();
//	}
//
//	@Test(expected = CoGrouperException.class)
//	public void testMissingSorting() throws CoGrouperException, IOException, InvalidFieldException {
//		new CoGrouperConfigBuilder().addSource(1, PangoolSchema.parse("url:string, date:long, content:string"))
//		    .addSource(2, PangoolSchema.parse("url:string, date:long, name:string, surname:string"))
//		    .setGroupByFields("url", "date").build();
//	}
//
//	@Test(expected = CoGrouperException.class)
//	public void testMissingGroupBy() throws CoGrouperException, InvalidFieldException, IOException {
//		new CoGrouperConfigBuilder().addSource(1, PangoolSchema.parse("url:string, date:long, content:string"))
//		    .addSource(2, PangoolSchema.parse("url:string, date:long, name:string, surname:string")).setSorting(getTestSorting())
//		    .build();
//	}
//
//	@Test(expected = CoGrouperException.class)
//	public void testMissingInputs() throws CoGrouperException, InvalidFieldException, IOException {
//		CoGrouperConfig config = new CoGrouperConfigBuilder()
//		    .addSource(1, PangoolSchema.parse("url:string, date:long, content:string"))
//		    .addSource(2, PangoolSchema.parse("url:string, date:long, name:string, surname:string"))
//		    .setGroupByFields("url", "date").setSorting(getTestSorting()).build();
//
//		new CoGrouper(config, new Configuration()).setGroupHandler(myGroupHandler.getClass())
//		    .setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class)
//		    .createJob();
//	}
//
//	@Test(expected = CoGrouperException.class)
//	public void testMissingOutput() throws CoGrouperException, InvalidFieldException, IOException {
//
//		CoGrouperConfig config = new CoGrouperConfigBuilder()
//		    .addSource(1, PangoolSchema.parse("url:string, date:long, content:string"))
//		    .addSource(2, PangoolSchema.parse("url:string, date:long, name:string, surname:string"))
//		    .setGroupByFields("url", "date").setSorting(getTestSorting()).build();
//
//		new CoGrouper(config, new Configuration())
//		    .addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
//		    .setGroupHandler(myGroupHandler.getClass()).createJob();
//	}
//
//	@Test(expected = CoGrouperException.class)
//	public void testMissingGroupHandler() throws CoGrouperException, InvalidFieldException, IOException {
//
//		CoGrouperConfig config = new CoGrouperConfigBuilder()
//		    .addSource(1, PangoolSchema.parse("url:string, date:long, content:string"))
//		    .addSource(2, PangoolSchema.parse("url:string, date:long, name:string, surname:string"))
//		    .setGroupByFields("url", "date").setSorting(getTestSorting()).build();
//
//		CoGrouper grouper = new CoGrouper(config, new Configuration()).addInput(new Path("input"), TextInputFormat.class,
//		    myInputProcessor.getClass()).setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
//
//		grouper.createJob();
//	}
//
//	@Test
//	public void testAllFine() throws CoGrouperException, InvalidFieldException, IOException {
//
//		CoGrouperConfig config = new CoGrouperConfigBuilder()
//		    .addSource(1, PangoolSchema.parse("url:string, date:long, content:string"))
//		    .addSource(2, PangoolSchema.parse("url:string, date:long, name:string, surname:string"))
//		    .setGroupByFields("url", "date").setSorting(getTestSorting()).build();
//
//		CoGrouper grouper = new CoGrouper(config, new Configuration())
//		    .addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
//		    .setGroupHandler(myGroupHandler.getClass())
//		    .setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
//
//		grouper.createJob();
//	}
//}
