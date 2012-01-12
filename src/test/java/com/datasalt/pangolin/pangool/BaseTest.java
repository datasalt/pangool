package com.datasalt.pangolin.pangool;

import java.io.IOException;

import org.junit.Before;

import com.datasalt.pangolin.commons.test.AbstractBaseTest;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;
import com.datasalt.pangolin.thrift.test.A;

public abstract class BaseTest extends AbstractBaseTest{

	public static Schema SCHEMA;

	@Before
	public void prepareBaseSchema() throws CoGrouperException, IOException {
		SCHEMA = Schema.parse(
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
}
