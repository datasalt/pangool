//package com.datasalt.pangool;
//
//import java.io.IOException;
//
//import junit.framework.Assert;
//
//import org.codehaus.jackson.JsonGenerationException;
//import org.codehaus.jackson.map.JsonMappingException;
//import org.codehaus.jackson.map.ObjectMapper;
//import org.junit.Test;
//
//import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
//import com.datasalt.pangool.Schema.Field;
//import com.datasalt.pangool.SortCriteria.SortOrder;
//
//public class TestCoGrouperConfig {
//
//	@Test(expected = CoGrouperException.class)
//	public void testSortingBySameFieldDifferentType() throws CoGrouperException, InvalidFieldException {
//		/*
//		 * fetched:long and fetched:vlong can't be sorted in common sorting
//		 */
//		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
//
//		configBuilder.addSchema(0, Schema.parse("url:string, fetched:long, content:string"));
//		configBuilder.addSchema(1, Schema.parse("url:string, fetched:vlong, name:string"));
//		configBuilder.setSorting(Sorting.parse("url asc, fetched desc"));
//		configBuilder.setGroupByFields("url");
//		configBuilder.build();
//	}
//	
//	@Test(expected = CoGrouperException.class)
//	public void testSourceIdNotAllowedForOneSchema() throws CoGrouperException, InvalidFieldException {
//		/*
//		 * We can't add #source# sorting if we only have one schema
//		 */
//		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
//
//		configBuilder.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
//		configBuilder.setSorting(Sorting.parse("url asc, fetched desc"));
//		configBuilder.setGroupByFields("url");
//		configBuilder.build();
//	}
//
//	@Test(expected = CoGrouperException.class)
//	public void testSpecificNotIncludedInCommonSorting() throws CoGrouperException, InvalidFieldException {
//		/*
//		 * If we sort by url all schemas, we can't sort by url one specific schema
//		 */
//		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
//
//		configBuilder.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
//		configBuilder.addSchema(1, Schema.parse("fetched:long, url:string, name:string"));
//		configBuilder.setSorting(new SortingBuilder().add("url", SortOrder.DESC)
//		    .secondarySort(1).add("url", SortOrder.ASC).buildSorting());
//		configBuilder.setGroupByFields("url");
//		configBuilder.build();
//	}
//	
//	@Test
//	public void testCommonFieldsInSpecificSorting() throws CoGrouperException, InvalidFieldException {
//		/*
//		 * Sorting by common fields in specific sortings is allowed
//		 * Types may differ
//		 */
//		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
//
//		configBuilder.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
//		configBuilder.addSchema(1, Schema.parse("fetched:vlong, url:string, name:string"));
//		configBuilder.setSorting(new SortingBuilder().add("url", SortOrder.DESC).addSourceId(SortOrder.ASC)
//		    .secondarySort(1).add("fetched", SortOrder.ASC).buildSorting());
//		configBuilder.setGroupByFields("url");
//		
//		CoGrouperConfig grouperConf = configBuilder.build();
//		
//		Assert.assertEquals(Schema.parse("url:string").toString(),
//		    grouperConf.getCommonOrderedSchema().toString());
//		Assert.assertEquals(Schema.parse("content:string, date:long, fetched:long").toString(), grouperConf.getSpecificOrderedSchemas().get(0).toString());
//		Assert.assertEquals(Schema.parse("fetched:vlong, name:string").toString(), grouperConf.getSpecificOrderedSchemas().get(1).toString());
//	}
//
//	@Test
//	public void testSourceIdAddedToCommonSchema() throws CoGrouperException, InvalidFieldException {
//		/*
//		 * Test that if we don't have specific sortings and we haven't added #source#, 
//		 * then it is automatically added to the end of common sorting
//		 */
//		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
//
//		configBuilder.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
//		configBuilder.addSchema(1, Schema.parse("fetched:long, url:string, name:string"));
//		configBuilder.setSorting(Sorting.parse("url asc, fetched desc"));
//		configBuilder.setGroupByFields("url");
//		CoGrouperConfig grouperConf = configBuilder.build();
//
//		Assert.assertEquals(Schema.parse("url:string, fetched:long").toString(),
//		    grouperConf.getSerializationInfo().getCommonSchema().toString());
//		Assert.assertEquals(Schema.parse("content:string, date:long").toString(),
//		    grouperConf.getSerializationInfo().getSpecificSchema(0).toString());
//		Assert.assertEquals(Schema.parse("name:string").toString(),
//		    grouperConf.getSerializationInfo().getSpecificSchema(1).toString());
//	}
//
//	@Test
//	public void testCommonOrderedSchemaWithSourceId() throws InvalidFieldException, CoGrouperException {
//		/*
//		 * We can put #source# sorting anywhere in the middle of common sorting
//		 */
//		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
//
//		configBuilder.addSchema(0, Schema.parse("url:string, date:long, fetched:long, content:string"));
//		configBuilder.addSchema(1, Schema.parse("fetched:long, url:string, name:string"));
//
//		configBuilder.setSorting(new SortingBuilder().add("url", SortOrder.ASC).add("fetched", SortOrder.DESC)
//		    .buildSorting());
//
//		configBuilder.setGroupByFields("url");
//		CoGrouperConfig grouperConf = configBuilder.build();
//
//		Assert.assertEquals(Schema.parse("url:string, " + Field.SOURCE_ID_FIELD_NAME + ":vint" + ", fetched:long").toString(),
//		    grouperConf.getCommonOrderedSchema().toString());
//		Assert.assertEquals(Schema.parse("content:string, date:long").toString(),
//		    grouperConf.getSpecificOrderedSchemas().get(0).toString());
//		Assert.assertEquals(Schema.parse("name:string").toString(),
//		    grouperConf.getSpecificOrderedSchemas().get(1).toString());
//	}
//
//	@Test
//	public void testSerDeEquality() throws JsonGenerationException, JsonMappingException, IOException,
//	    CoGrouperException, InvalidFieldException {
//		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
//
//		SchemaBuilder builder1 = new SchemaBuilder();
//		builder1.add("url", String.class).add("date", Long.class).add("content", String.class);
//
//		SchemaBuilder builder2 = new SchemaBuilder();
//		builder2.add("url", String.class).add("date", Long.class).add("name", String.class);
//
//		SortingBuilder builder = new SortingBuilder();
//		Sorting sorting = builder.add("url", SortOrder.ASC).add("date", SortOrder.DESC).addSourceId(SortOrder.ASC)
//		    .secondarySort(1).add("content", SortOrder.ASC).secondarySort(2).add("name", SortOrder.ASC).buildSorting();
//
//		configBuilder.addSchema(1, builder1.createSchema());
//		configBuilder.addSchema(2, builder2.createSchema());
//		configBuilder.setSorting(sorting);
//		configBuilder.setRollupFrom("url");
//		configBuilder.setGroupByFields("url", "date");
//		CoGrouperConfig grouperConf = configBuilder.build();
//
//		ObjectMapper mapper = new ObjectMapper();
//		String jsonConfig = grouperConf.toStringAsJSON(mapper);
//		CoGrouperConfig config2 = CoGrouperConfigBuilder.fromJSON(jsonConfig, mapper);
//
//		Assert.assertEquals(jsonConfig, config2.toStringAsJSON(mapper));
//	}
//}
