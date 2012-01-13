package com.datasalt.pangolin.pangool;

import junit.framework.Assert;

import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;

/**
 * 
 * @author pere
 *
 */
public class TestSchemaBuilder {

	@Test
	public void testInvalidDuplicatedField() throws InvalidFieldException {
		SchemaBuilder schema = new SchemaBuilder();
		schema.add("url", String.class);
		try {
			schema.add("url", Integer.class);
			Assert.fail("Catched exception after adding same field twice should have been thrown");
		} catch(Exception ife) {
			return;
		}
	}

	@Test
	public void testInvalidReservedName() {
		SchemaBuilder schema = new SchemaBuilder();
		try {
			schema.add(Schema.Field.SOURCE_ID_FIELD_NAME, String.class);
			Assert.fail("Catched exception after adding reserved field should have been thrown");
		} catch(Exception ife) {
			return;
		}
	}	
	
	public void testInvalidClass() {
		SchemaBuilder schema = new SchemaBuilder();
		try {
			schema.add("whatever", null);
			Assert.fail("Catched exception after adding null class should have been thrown");
		} catch(Exception ife) {
			return;
		}		
	}
	
	@Test
	public void testCreateSchema() throws InvalidFieldException {
		SchemaBuilder schema = new SchemaBuilder();
		schema.add("strField", String.class);
		schema.add("intField", Integer.class);
		schema.add("floatField", Float.class);
		Schema sch = schema.createSchema();
		Assert.assertNotNull(sch);
		Assert.assertEquals(String.class, sch.getField("strField").getType());
		Assert.assertEquals(Integer.class, sch.getField("intField").getType());
		Assert.assertEquals(Float.class, sch.getField("floatField").getType());
	}
}
