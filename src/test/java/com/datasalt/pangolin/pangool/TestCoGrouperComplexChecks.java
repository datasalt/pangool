package com.datasalt.pangolin.pangool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;

public class TestCoGrouperComplexChecks extends BaseCoGrouperTest {

	@Test(expected = CoGrouperException.class)
	public void testSortingWithUncommonElement() throws InvalidFieldException, CoGrouperException {
		Sorting sorting = Sorting.parse("url asc, content desc"); // not in common: content
		testCoGrouper(sorting, new String[] { "url" }, null);
	}

	@Test(expected = CoGrouperException.class)
	public void testSortingWithUnexistingElement() throws InvalidFieldException, CoGrouperException {
		Sorting sorting = Sorting.parse("url asc, foo desc"); // unexisting: foo
		testCoGrouper(sorting, new String[] { "url" }, null);
	}

	@Test
	public void testValidSortings() throws InvalidFieldException, CoGrouperException {
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
	public void testSecondarySortingUnexistingField() throws InvalidFieldException, CoGrouperException {
		Sorting sorting = new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.ASC)
			.secondarySort(1).add("foo", SortOrder.DESC)
			.secondarySort(2).add("name", SortOrder.DESC)
			.buildSorting();

		testCoGrouper(sorting, new String[] { "url", "date" }, null);
	}
	
	@Test
	public void testSecondarySortings() throws InvalidFieldException, CoGrouperException {
		Sorting sorting = new SortingBuilder()
			.add("url", SortOrder.ASC)
			.add("date", SortOrder.ASC)
			.secondarySort(1).add("content", SortOrder.DESC)
			.secondarySort(2).add("name", SortOrder.DESC)
			.buildSorting();

		testCoGrouper(sorting, new String[] { "url", "date" }, null);
	}
	
	// --------------------------------------------------- //
	
	@Test
	public void testValidRollupFrom() throws CoGrouperException {
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
	public void testInvalidRollupFrom() throws CoGrouperException {
		Sorting sorting;
		
		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date" }, "fetched");
		
		sorting = Sorting.parse("url asc, date asc, fetched desc");
		testCoGrouper(sorting, new String[] { "url", "date" }, "foo");
	}
	
	private void testCoGrouper(Sorting sorting, String[] groupBy, String rollupFrom) throws CoGrouperException {
		CoGrouper grouper = new CoGrouper(new Configuration());

		grouper
			.addSchema(1, "url:string, date:long, fetched:long, content:string")
		  .addSchema(2, "url:string, date:long, fetched:long, name:string, surname:string")
		  .setSorting(sorting)
		  .groupBy(groupBy)
		  .addInput(new Path("input"), TextInputFormat.class, myInputProcessor.getClass())
		  .setGroupHandler(myGroupHandlerWithRollup.getClass())
		  .setOutput(new Path("output"), TextOutputFormat.class, Object.class, Object.class);
		
		if(rollupFrom != null) {
			grouper.setRollupFrom(rollupFrom);
		}

		grouper.doAllChecks();
	}
}
