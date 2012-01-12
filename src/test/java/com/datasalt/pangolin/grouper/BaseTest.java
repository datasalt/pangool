package com.datasalt.pangolin.grouper;

import java.io.IOException;

import org.junit.Before;

import com.datasalt.pangolin.commons.test.AbstractBaseTest;
import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;

public abstract class BaseTest extends AbstractBaseTest{

	public static Schema SCHEMA;

	@Before
	public void prepare2() throws GrouperException, IOException {
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
		
				Schema.setInConfig(SCHEMA, getConf());
	}
	
}
