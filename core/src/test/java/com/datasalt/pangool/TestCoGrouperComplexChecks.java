package com.datasalt.pangool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Sorting;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.SortCriteria.SortOrder;

public class TestCoGrouperComplexChecks extends BaseCoGrouperTest {

	@Test(expected = CoGrouperException.class)
	public void testSortingWithUncommonElement() throws InvalidFieldException, CoGrouperException, IOException {
		Sorting sorting = Sorting.parse("url asc, content desc"); // not in common: content
		testCoGrouper(sorting, new String[] { "url" }, null);
	}

	@Test(expected = CoGrouperException.class)
	public void testSortingWithUnexistingElement() throws InvalidFieldException, CoGrouperException, IOException {
		Sorting sorting = Sorting.parse("url asc, foo desc"); // unexisting: foo
		testCoGrouper(sorting, new String[] { "url" }, null);
	}

	@Test
	public void testValidSortings() throws InvalidFieldException, CoGrouperException, IOException {
		Sorting sorting;
		sorting = Sorting.parse("url asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url" }, null);
		sorting = Sorting.parse("url asc, date desc");
		testCoGrouper(sorting, new String[] { "url" }, null);
		sorting = Sorting.parse("date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "date", "fetched" }, null);
		sorting = Sorting.parse("fetched asc, date desc");
		testCoGrouper(sorting, new String[] { "fetched" }, null);
	}

	@Test(expected = CoGrouperException.class)
	public void testSecondarySortingUnexistingField() throws InvalidFieldException, CoGrouperException, IOException {
		Sorting sorting = new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.ASC)
			.addSourceId(SortOrder.DESC)
			.secondarySort(1).add("foo", SortOrder.DESC)
			.secondarySort(2).add("name", SortOrder.DESC)
			.buildSorting();

		testCoGrouper(sorting, new String[] { "url", "date" }, null);
	}
	
	@Test
	public void testSecondarySortings() throws InvalidFieldException, CoGrouperException, IOException {
		Sorting sorting = new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.ASC)
			.addSourceId(SortOrder.DESC)
			.secondarySort(1).add("content", SortOrder.DESC)
			.secondarySort(2).add("name", SortOrder.DESC)
			.buildSorting();

		testCoGrouper(sorting, new String[] { "url", "date" }, null);
	}
	
	// --------------------------------------------------- //
	
	@Test
	public void testValidRollupFrom() throws CoGrouperException, IOException, InvalidFieldException {
		Sorting sorting;
		
		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date" }, "url");
		
		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date" }, "date");

		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date", "fetched" }, "url");

		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date", "fetched" }, "date");

		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date", "fetched" }, "fetched");
	}

	// --------------------------------------------------- //

	@Test(expected=CoGrouperException.class)
	public void testInvalidRollupFrom() throws CoGrouperException, IOException, InvalidFieldException {
		Sorting sorting;
		
		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date" }, "fetched");
		
		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date" }, "foo");
	}
	
	private void testCoGrouper(Sorting sorting, String[] groupBy, String rollupFrom) throws CoGrouperException, IOException, InvalidFieldException {
		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder()
			.addSchema(1, Schema.parse("url:string, date:long, fetched:long, content:string"))
		  .addSchema(2, Schema.parse("url:string, date:long, fetched:long, name:string, surname:string"))
		  .setSorting(sorting)
		  .setGroupByFields(groupBy);
		  
		if(rollupFrom != null) {
			configBuilder.setRollupFrom(rollupFrom);
		}

		CoGrouper grouper = new CoGrouper(configBuilder.build(), new Configuration());

		grouper.addInput(new Path("input"), TextInputFormat.class, myInputProcessor)
		  .setGroupHandler(myGroupHandlerWithRollup)
		  .setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class)
		  .createJob();
	}
}
