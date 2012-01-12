package com.datasalt.pangolin.grouper.io.tuple;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datasalt.pangolin.grouper.BaseTest;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;
import com.datasalt.pangolin.grouper.io.tuple.serialization.TupleSerialization;
import com.datasalt.pangolin.thrift.test.A;

public class TestTuple extends BaseTest{

	enum TestEnum {
		S,Blabla
	};
	
	
	@Test
	public void testRandomTupleSerialization() throws IOException {
			BaseTuple baseTuple = new BaseTuple();
			Tuple doubleBufferedTuple = new Tuple();
			ITuple[] tuples = new ITuple[]{baseTuple,doubleBufferedTuple};
			int NUM_ITERATIONS=10000;
			for (int i=0 ; i < NUM_ITERATIONS; i++){
			for (ITuple tuple : tuples){
				fillWithRandom(SCHEMA, tuple, 0, SCHEMA.getFields().length-1);
				assertSerializable(tuple,false);
			}
		}
	}
	
	@Test
	public void testMissingTypes() throws GrouperException, IOException{
		
		Schema schemaPrimitives = Schema.parse(
				"int_field:int,"+
				"long_field:long," + 
				"vint_field:vint," + 
				"vlong_field:vlong," +
				"float_field:float," +
				"double_field:double," + 
		    "boolean_field:boolean," + 
		    "enum_field:" + SortOrder.class.getName());
				Schema.setInConfig(schemaPrimitives, getConf());
		
		
		//ITuple[] tuples = new ITuple[]{baseTuple,doubleBufferedTuple};
		
		int length = schemaPrimitives.getFields().length;
		for (int i=length-2 ; i >= 0; i--){
			System.out.println("From 0 to " + i);
			ITuple tuple = new BaseTuple();
			fillWithRandom(schemaPrimitives, tuple, 0, i);
			assertNotSerializable(tuple);
			tuple = new Tuple();
			fillWithRandom(schemaPrimitives, tuple, 0, i);
			assertNotSerializable(tuple);
		}

	}
	
	
//	@Ignore
//	@Test
//	public void testTupleStorage() throws GrouperException, IOException{
//		
//		
//		Random random = new Random();
//		BaseTuple baseTuple = new BaseTuple();
//		Tuple doubleBufferedTuple = new Tuple();
//		ITuple[] tuples = new ITuple[]{baseTuple,doubleBufferedTuple};
//		
//		
//		
//		
//		for (ITuple tuple : tuples){
//			System.out.println(tuple);
//			//check if they can be serializable with no fields set
//			assertSerializable(tuple,false);
//		}
//		
//		
//		
//		
//		for(ITuple tuple : tuples) {
//			int value = random.nextInt();
//			tuple.setInt("int_field", value);
//			assertEquals(value, tuple.getInt("int_field"));
//			assertEquals(value, tuple.getObject("int_field"));
//			value = random.nextInt();
//			tuple.setObject("int_field", value);
//			assertEquals(value, tuple.getInt("int_field"));
//			assertEquals(value, tuple.getObject("int_field"));
//
//			
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//		}
//		
//		
//		
//		for (ITuple tuple : tuples){
//			int value = random.nextInt();
//			tuple.setInt("vint_field",value);
//			assertEquals(value,tuple.getInt("vint_field"));
//			assertEquals(value,tuple.getObject("vint_field"));
//			value = random.nextInt();
//			tuple.setObject("vint_field",value);
//			assertEquals(value,tuple.getInt("vint_field"));
//			assertEquals(value,tuple.getObject("vint_field"));
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//		}
//		
//		
//		for (ITuple tuple : tuples){
//			long value = random.nextLong();
//			tuple.setLong("long_field",value);
//			assertEquals(value,tuple.getLong("long_field"));
//			assertEquals(value,tuple.getObject("long_field"));
//			value = random.nextLong();
//			tuple.setObject("long_field",value);
//			assertEquals(value,tuple.getLong("long_field"));
//			assertEquals(value,tuple.getObject("long_field"));
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//		}
//		
//		for (ITuple tuple : tuples){
//			long value = random.nextLong();
//			tuple.setLong("vlong_field",value);
//			assertEquals(value,tuple.getLong("vlong_field"));
//			assertEquals(value,tuple.getObject("vlong_field"));
//			value = random.nextLong();
//			tuple.setObject("vlong_field",value);
//			assertEquals(value,tuple.getLong("vlong_field"));
//			assertEquals(value,tuple.getObject("vlong_field"));
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//		}
//		for (ITuple tuple : tuples){
//			String value = "caca";
//			tuple.setString("string_field",value);
//			assertEquals(value,tuple.getString("string_field"));
//			assertEquals(value,tuple.getObject("string_field"));
//			value = "cucu";
//			tuple.setObject("string_field",value);
//			assertEquals(value,tuple.getString("string_field"));
//			assertEquals(value,tuple.getObject("string_field"));
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//
//		}
//		
//		for (ITuple tuple : tuples){
//			float value = random.nextFloat();
//			tuple.setFloat("float_field",value);
//			assertEquals(value,tuple.getFloat("float_field"),1e-10);
//			assertEquals(value,(Float)tuple.getObject("float_field"),1e-10);
//			value = random.nextFloat();
//			tuple.setObject("float_field",value);
//			assertEquals(value,tuple.getFloat("float_field"),1e-10);
//			assertEquals(value,(Float)tuple.getObject("float_field"),1e-10);
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//
//		}
//		for (ITuple tuple : tuples){
//			double value = random.nextDouble();
//			tuple.setDouble("double_field",value);
//			assertEquals(value,tuple.getDouble("double_field"),1e-10);
//			assertEquals(value,(Double)tuple.getObject("double_field"),1e-10);
//			value = random.nextDouble();
//			tuple.setObject("double_field",value);
//			assertEquals(value,tuple.getDouble("double_field"),1e-10);
//			assertEquals(value,(Double)tuple.getObject("double_field"),1e-10);
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//		}
//		
//		for (ITuple tuple : tuples){
//			SortOrder value = SortOrder.ASC;
//			tuple.setEnum("enum_field",value);
//			assertEquals(value,tuple.getEnum("enum_field"));
//			assertEquals(value,tuple.getObject("enum_field"));
//			
//		 TestEnum value2 = TestEnum.Blabla;
//			
//		 tuple.setEnum("enum_field",value2);
//			assertEquals(value2,tuple.getEnum("enum_field"));
//			assertEquals(value2,tuple.getObject("enum_field"));
//			
//			tuple.setObject("enum_field",value);
//			assertEquals(value,tuple.getEnum("enum_field"));
//			assertEquals(value,tuple.getObject("enum_field"));
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//
//		}
//		
//		for (ITuple tuple : tuples){
//			A value = new A();
//			value.setId("id");
//			tuple.setObject("thrift_field",value);
//			assertEquals(value,tuple.getObject("thrift_field"));
//			assertEquals(value.getId(),((A)tuple.getObject("thrift_field")).getId());
//			value = new A();
//			value.setId("id2");
//			tuple.setObject("thrift_field",value);
//			assertEquals(value,tuple.getObject("thrift_field"));
//			assertEquals(value.getId(),((A)tuple.getObject("thrift_field")).getId());
//			System.out.println(tuple);
//			assertSerializable(tuple,false);
//		}
//		
//		//TODO what should happen when assign an int,short  to a long (automatic conversion(casting) or exception?)
//		//TODO what happens if we retrieve a long using getInt  , or a int using getLong ?
//		
//		//TODO should we convert float to double ?
//	}
	
//	private void assertLeftEquals(ITuple tuple1,ITuple tuple2){
//		if (!BaseTuple.leftEquals(tuple1, tuple2)){
//			Assert.fail("tuples are not left equals\n" + tuple1 + "\n" + tuple2);
//		}
//	}
	
	
	private void assertSerializable(ITuple tuple,boolean debug) throws IOException{
		TupleSerialization serialization = new TupleSerialization();
		serialization.setConf(getConf());

		Serializer<ITuple> ser = serialization.getSerializer(ITuple.class);
		Deserializer<ITuple> deser = serialization.getDeserializer(ITuple.class);

		DataInputBuffer input = new DataInputBuffer();
		DataOutputBuffer output = new DataOutputBuffer();

		ser.open(output);
	  ser.serialize(tuple);
	  ser.close();
    
	  input.reset(output.getData(),0,output.getLength());
		ITuple deserializedTuple = new BaseTuple();
		deser.open(input);
		deserializedTuple = deser.deserialize(deserializedTuple);
		if (debug){
			System.out.println("D:" + deserializedTuple);
		}
		deser.close();
		assertEquals(tuple,deserializedTuple);
	  deserializedTuple = new Tuple();
	  
	  input.reset(output.getData(),0,output.getLength());
	  deser.open(input);
		deserializedTuple = deser.deserialize(deserializedTuple);
		deser.close();
		if (debug){
			System.out.println("D2:" + deserializedTuple);
		}

		assertEquals(tuple,deserializedTuple);
	}
	
	
	
//	@Test
//	public void testAssingWrongTypes(){
//		
//		//Ituple
//		
////		try{
////			//can't assign wrong types
////		  tuple.setString("int_field","caca");
////		  Assert.fail();
////		} catch(InvalidFieldException e){
////			e.printStackTrace();
////		}
////		
////		try {
////			tuple.setObject("string_field", new A());
////		} catch(InvalidFieldException e){
////			e.printStackTrace();
////		}
////		
////		
////	}
//
//	}
		
	
	private void assertNotSerializable(ITuple tuple){
		try{
		assertSerializable(tuple,false);
		Assert.fail();
		} catch(Exception e){
			System.out.println(e);
		}
	}
	
//	@Ignore
//	@Test
//	/**
//	 * Can't serialize nulls to primitive types (ints,floats..)
//	 */
//	public void testPrimitivesNonnull() throws GrouperException, IOException {
//
//		ITuple baseTuple = new BaseTuple();
//		ITuple doubleBufferedTuple = new Tuple();
//		ITuple[] tuples = new ITuple[]{baseTuple,doubleBufferedTuple};
//		
//		for(ITuple tuple : tuples){
//			tuple.setObject("int_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("int_field", 3);
//			tuple.setObject("vint_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("vint_field", 10);
//			tuple.setObject("long_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("long_field", 11l);
//			tuple.setObject("vlong_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("vlong_field", 12l);
//			tuple.setObject("double_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("double_field", 12.0);
//			tuple.setObject("float_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("float_field", 12f);
//			tuple.setObject("boolean_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("boolean_field", true);
//			tuple.setObject("enum_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("enum_field", SortOrder.ASC);
//			tuple.setObject("string_field", null);
//			assertNotSerializable(tuple);
//			tuple.setObject("string_field", "");
//		}
//		
//	}
//	
//	
//	
	
	
//	@Test
//	public void testNullSchema() throws IOException{
//		ITuple baseTuple = ReflectionUtils.newInstance(BaseTuple.class,null);
//		ITuple doubleBufferedTuple =ReflectionUtils.newInstance(Tuple.class,null);
//		ITuple[] tuples = new ITuple[]{baseTuple,doubleBufferedTuple};
//		
//		for (ITuple tuple : tuples){
//		try{
//			tuple.setSchema(null);
//			Assert.fail();
//		} catch(Exception e){
//			System.out.println(e);
//		}
//		}
//		
//		
//		for (ITuple tuple : tuples){
//		tuple.setSchema(SCHEMA);
//		try{
//			tuple.setSchema(SCHEMA); //can't assign twice an SCHEMA 
//		} catch(IllegalStateException e){
//			System.out.println(e);
//		}
//		}
//		
//		baseTuple = ReflectionUtils.newInstance(BaseTuple.class,null);
//		doubleBufferedTuple =ReflectionUtils.newInstance(Tuple.class,null);
//		tuples = new ITuple[]{baseTuple,doubleBufferedTuple};
//		
//		for (ITuple tuple : tuples){
//			tuple.setSchema(SCHEMA);
//			
//			try{
//				tuple.setConf(getConf()); //can't assign a configuration after SCHEMA set 
//			} catch(IllegalStateException e){
//				System.out.println(e);
//			}
//			}
//		
//		
//	}

}
