package com.datasalt.pangolin.grouper;

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.thrift.test.A;

public class TestTuple {

	private FieldsDescription schema;

	@Before
	public void prepare() throws GrouperException {
		schema = FieldsDescription.parse(
				"int_field:int,"+
				"long_field:long," + 
				"vint_field:vint," + 
				"vlong_field:vlong," +
				"float_field:float," +
				"double_field:double," + 
		    "string_field:string," + 
		    "boolean_field:boolean," + 
		    "thrift_field:" + A.class.getName());
	}

	@Test
	public void testTupleStorage() throws GrouperException{
		
		
		
		Random random = new Random();
		Tuple tuple = new Tuple(schema);
		{
			int value = random.nextInt();
			tuple.setInt("int_field",value);
			assertEquals(value,tuple.getInt("int_field"));
			assertEquals(value,tuple.getObject("int_field"));
			value = random.nextInt();
			tuple.setObject("int_field",value);
			assertEquals(value,tuple.getInt("int_field"));
			assertEquals(value,tuple.getObject("int_field"));
		}
		
		{
			int value = random.nextInt();
			tuple.setInt("vint_field",value);
			assertEquals(value,tuple.getInt("vint_field"));
			assertEquals(value,tuple.getObject("vint_field"));
			value = random.nextInt();
			tuple.setObject("vint_field",value);
			assertEquals(value,tuple.getInt("vint_field"));
			assertEquals(value,tuple.getObject("vint_field"));
		}
		
		{
			long value = random.nextLong();
			tuple.setLong("long_field",value);
			assertEquals(value,tuple.getLong("long_field"));
			assertEquals(value,tuple.getObject("long_field"));
			value = random.nextLong();
			tuple.setObject("long_field",value);
			assertEquals(value,tuple.getLong("long_field"));
			assertEquals(value,tuple.getObject("long_field"));
		}
		
		{
			long value = random.nextLong();
			tuple.setLong("vlong_field",value);
			assertEquals(value,tuple.getLong("vlong_field"));
			assertEquals(value,tuple.getObject("vlong_field"));
			value = random.nextLong();
			tuple.setObject("vlong_field",value);
			assertEquals(value,tuple.getLong("vlong_field"));
			assertEquals(value,tuple.getObject("vlong_field"));
		}
		
		{
			String value = "caca";
			tuple.setString("string_field",value);
			assertEquals(value,tuple.getString("string_field"));
			assertEquals(value,tuple.getObject("string_field"));
			value = "cucu";
			tuple.setObject("string_field",value);
			assertEquals(value,tuple.getString("string_field"));
			assertEquals(value,tuple.getObject("string_field"));
		}
		
		
		{
			float value = random.nextFloat();
			tuple.setFloat("float_field",value);
			assertEquals(value,tuple.getFloat("float_field"),1e-10);
			assertEquals(value,(Float)tuple.getObject("float_field"),1e-10);
			value = random.nextFloat();
			tuple.setObject("float_field",value);
			assertEquals(value,tuple.getFloat("float_field"),1e-10);
			assertEquals(value,(Float)tuple.getObject("float_field"),1e-10);
		}
		
		{
			double value = random.nextDouble();
			tuple.setDouble("double_field",value);
			assertEquals(value,tuple.getDouble("double_field"),1e-10);
			assertEquals(value,(Double)tuple.getObject("double_field"),1e-10);
			value = random.nextDouble();
			tuple.setObject("double_field",value);
			assertEquals(value,tuple.getDouble("double_field"),1e-10);
			assertEquals(value,(Double)tuple.getObject("double_field"),1e-10);
		}
		
		A a = new A();
		
		
		
		//TODO what should happen when assign an int,short  to a long (automatic conversion(casting) or exception?)
		//TODO what happens if we retrieve a long using getInt  , or a int using getLong ?
		
		//TODO should we convert float to double ?
		
		
		try{
			//can't assign string to field that is int
		  tuple.setString("user_id","caca");
		  Assert.fail();
		} catch(GrouperException e){}
		
		try {
			tuple.setObject("user_id", new A());
		} catch(GrouperException e){}
		
		
	}

	@Test
	/**
	 * Can assign nulls to primitive types
	 */
	public void testNonnull() throws GrouperException {

		Tuple tuple = new Tuple(schema);

		try {
			tuple.setObject("user_id", null);
			Assert.fail();
		} catch(GrouperException e) {
		}

		try {
			tuple.setObject("user_id", null);
			Assert.fail();
		} catch(GrouperException e) {
		}

		try {
			tuple.setObject("user_id", null);
			Assert.fail();
		} catch(GrouperException e) {
		}

		try {
			tuple.setObject("user_id", null);
			Assert.fail();
		} catch(GrouperException e) {
		}

	}

}
