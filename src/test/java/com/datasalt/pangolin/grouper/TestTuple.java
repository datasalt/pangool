package com.datasalt.pangolin.grouper;

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.Tuple.InvalidFieldException;
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
		    "enum_field:" + SortOrder.class.getName() + "," +
		    "thrift_field:" + A.class.getName());
	}

	enum TestEnum {
		S,Blabla
	};
	
	@Test
	public void testTupleStorage() throws GrouperException{
		
		
		
		Random random = new Random();
		Tuple tuple = new Tuple(schema);
		
		System.out.println(tuple.toString());
		
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
		
		System.out.println(tuple);
		
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
		System.out.println(tuple);
		
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
		System.out.println(tuple);
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
		System.out.println(tuple);
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
		System.out.println(tuple);
		
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
		System.out.println(tuple);
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
		System.out.println(tuple);
		
		{
			
			SortOrder value = SortOrder.ASCENDING;
			
			tuple.setEnum("enum_field",value);
			assertEquals(value,tuple.getEnum("enum_field"));
			assertEquals(value,tuple.getObject("enum_field"));
			
		 TestEnum value2 = TestEnum.Blabla;
			
		 tuple.setEnum("enum_field",value2);
			assertEquals(value2,tuple.getEnum("enum_field"));
			assertEquals(value2,tuple.getObject("enum_field"));
			
			tuple.setObject("enum_field",value);
			assertEquals(value,tuple.getEnum("enum_field"));
			assertEquals(value,tuple.getObject("enum_field"));
		}
		
		
		
		{
			
			A value = new A();
			value.setId("id");
			tuple.setObject("thrift_field",value);
			assertEquals(value,tuple.getObject("thrift_field"));
			assertEquals(value.getId(),((A)tuple.getObject("thrift_field")).getId());
			value = new A();
			value.setId("id2");
			tuple.setObject("thrift_field",value);
			assertEquals(value,tuple.getObject("thrift_field"));
			assertEquals(value.getId(),((A)tuple.getObject("thrift_field")).getId());
			
		}
		System.out.println(tuple);
		
		
		
		
		
		//TODO what should happen when assign an int,short  to a long (automatic conversion(casting) or exception?)
		//TODO what happens if we retrieve a long using getInt  , or a int using getLong ?
		
		//TODO should we convert float to double ?
		
		
		try{
			//can't assign wrong types
		  tuple.setString("int_field","caca");
		  Assert.fail();
		} catch(InvalidFieldException e){}
		
		try {
			tuple.setObject("string_field", new A());
		} catch(InvalidFieldException e){}
		
		
	}

	@Test
	/**
	 * Can assign nulls to primitive types
	 */
	public void testPrimitivesNonnull() throws GrouperException {

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
