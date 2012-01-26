package com.datasalt.avrool.test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Random;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;


import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Schema;
import com.datasalt.avrool.SortCriteria.SortOrder;
import com.datasalt.avrool.io.tuple.ITuple;
import com.datasalt.avrool.test.AbstractBaseTest;
import com.datasalt.avrool.thrift.test.A;

public abstract class BaseTest extends AbstractBaseTest{

//	public static Schema SCHEMA;
	private static long randomSeed = 123;
//
//	@Before
//	public void prepare2() throws CoGrouperException, IOException {
//		SCHEMA = Schema.parse(
//				"int_field:int,"+
//				"long_field:long," + 
//				"vint_field:vint," + 
//				"vlong_field:vlong," +
//				"float_field:float," +
//				"double_field:double," + 
//		    "string_field:string," + 
//		    "boolean_field:boolean," + 
//		    "enum_field:" + SortOrder.class.getName() + "," +
//		    "thrift_field:" + A.class.getName());
//		
//				Schema.setInConfig(SCHEMA, getConf());
//	}
	
//	/**
//	 * Fills the fields specified by the range (minIndex,maxIndex) with random
//	 * data.
//	 * 
//	 */
//	public static void fillTuple(boolean isRandom,Schema schema,ITuple tuple, int minIndex, int maxIndex) {
//		try {
//			Random random = new Random(randomSeed);
//			//Schema schema = tuple.getSchema();
//			for (int i = minIndex; i <= maxIndex; i++) {
//				Field field = schema.getField(i);
//				String fieldName = field.getName();
//				Class fieldType = field.getType();
//				if (fieldType == Integer.class || fieldType == VIntWritable.class) {
//					tuple.setInt(fieldName, isRandom ? random.nextInt() : 0);
//				} else if (fieldType == Long.class || fieldType == VLongWritable.class) {
//					tuple.setLong(fieldName, isRandom ? random.nextLong() : 0);
//				} else if (fieldType == Boolean.class) {
//					tuple.setBoolean(fieldName, isRandom ? random.nextBoolean() : false);
//				} else if (fieldType == Double.class) {
//					tuple.setDouble(fieldName, isRandom ? random.nextDouble() : 0.0);
//				} else if (fieldType == Float.class) {
//					tuple.setFloat(fieldName, isRandom ? random.nextFloat() : 0f);
//				} else if (fieldType == String.class) {
//					if (!isRandom || random.nextBoolean()) {
//						tuple.setString(fieldName, "");
//					} else {
//						tuple.setString(fieldName, random.nextLong() + "");
//					}
//				} else if (fieldType.isEnum()) {
//					Method method = fieldType.getMethod("values", null);
//					Enum[] values = (Enum[]) method.invoke(null);
//					tuple.setEnum(fieldName, values[isRandom ? random.nextInt(values.length) : 0]);
//				} else {
//					boolean toInstance = random.nextBoolean();
//					if (isRandom && toInstance) {
//						Object instance = ReflectionUtils.newInstance(fieldType, null);
//						tuple.setObject(fieldName, instance);
//					} else {
//						tuple.setObject(fieldName, null);
//					}
//				}
//			}
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
	
}
