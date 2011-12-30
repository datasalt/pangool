/**
 * Copyright [2011] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasalt.pangolin.grouper.io.tuple;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangolin.grouper.BaseGrouperTest;
import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.FieldsDescription.Field;
import com.datasalt.pangolin.grouper.FieldsDescriptionBuilder;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.SortCriteria;
import com.datasalt.pangolin.grouper.SortCriteria.SortElement;
import com.datasalt.pangolin.grouper.SortCriteria.SortOrder;
import com.datasalt.pangolin.grouper.SortCriteriaBuilder;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.thrift.test.A;


public class TestTupleComparator extends BaseGrouperTest{
	
	private static class AComparator implements RawComparator<com.datasalt.pangolin.thrift.test.A>,Configurable {

		private Configuration conf;
		private Serialization ser;
		
		private A a1=new A();
		private A a2=new A();
		
		

		@Override
    public int compare(A o1, A o2) {
			int comparison= o1.getId().compareTo(o2.getId());
			if (comparison == 0){
				comparison = o1.getUrl().compareTo(o2.getUrl());
			}
			return comparison;
    }

		@Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
	    try {
	      a1 = ser.deser(a1,b1,s1, l1);
  	    a1 = ser.deser(a2,b2,s2,l2);
  	    return compare(a1,a2);
	    } catch(IOException e) {
	     throw new RuntimeException(e);
      }
    }

		@Override
    public void setConf(Configuration conf) {
	    if (conf != null){
	    	this.conf = conf;
	    	try {
	        this.ser = new Serialization(conf);
        } catch(IOException e) {
	        throw new RuntimeException(e);
        }
	    }
    }

		@Override
    public Configuration getConf() {
	   return conf;
    }
	}
	
	
	private void fillWithRandom(ITuple tuple,int minIndex,int maxIndex){
		try{
		Random random = new Random();
		FieldsDescription fieldsDescription = tuple.getSchema();
		for (int i=minIndex ; i <= maxIndex ; i++){
			Field field = fieldsDescription.getField(i);
			String fieldName = field.getName();
			Class fieldType = field.getType();
			if (fieldType == Integer.class || fieldType == VIntWritable.class){
				tuple.setInt(fieldName, random.nextInt());
			} else if(fieldType == Long.class || fieldType == VLongWritable.class){
				tuple.setLong(fieldName,random.nextLong());
			} else if(fieldType == Boolean.class){
				tuple.setBoolean(fieldName, random.nextBoolean());
			} else if (fieldType == Double.class){
				tuple.setDouble(fieldName, random.nextDouble());
			} else if (fieldType == Float.class){
				tuple.setFloat(fieldName, random.nextFloat());
			} else if (fieldType == String.class){
				if (random.nextBoolean()){
					tuple.setString(fieldName,"");
				} else {
					tuple.setString(fieldName,random.nextLong()+"");
				}
			} else if (fieldType.isEnum()){
				Method method = fieldType.getMethod("values", null);
				Enum[] values = (Enum[])method.invoke(null);
				tuple.setEnum(fieldName, values[random.nextInt(values.length)]);
			} else {
				//TODO add more things
			}
		}
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	private static FieldsDescription permuteSchema(FieldsDescription schema){
		Field[] fields = schema.getFields();
		List<Field> permutedFields = Arrays.asList(fields);
		Collections.shuffle(permutedFields);
		FieldsDescriptionBuilder builder = new FieldsDescriptionBuilder();
		for (Field field : permutedFields){
			try {
	      builder.addField(field.getName(), field.getType());
      } catch(InvalidFieldException e) {
      	throw new RuntimeException(e);
      }
		}
		return builder.createFieldsDescription();
	}
	
	private static SortCriteria createRandomSortCriteria(FieldsDescription schema,int numFields){
		try {
		Random random = new Random();
		SortCriteriaBuilder builder = new SortCriteriaBuilder();
		for (int i = 0 ; i < numFields ; i++){
			Field field = schema.getField(i);
			  builder.addSortElement(field.getName(), random.nextBoolean() ? SortOrder.ASCENDING : SortOrder.DESCENDING);
		}
		return builder.createSortCriteria();
		} catch(InvalidFieldException e) {
      throw new RuntimeException(e);
    }
	}
	
	private String[] getFirstFields(SortCriteria sortCriteria,int numFields){
		String[] result = new String[numFields];
		for (int i=0 ; i < numFields ; i++){
			SortElement element = sortCriteria.getSortElements()[i];
			result[i] = element.getFieldName();
		}
		return result;
	}
	
	@Test
	public void test() throws GrouperException, IOException{
		Configuration conf=getConf();
		
		
		int maxIndex =SCHEMA.getFields().length-1; 
		int MAX_RANDOMS_PER_INDEX = 10;
		for (int minIndex= maxIndex ; minIndex>=0 ; minIndex--){
			SortCriteria sortCriteria = createRandomSortCriteria(SCHEMA,maxIndex+1);
			SortCriteria.setInConfig(sortCriteria, conf);
			SortComparator sortComparator = new SortComparator();
			GroupComparator groupComparator = new GroupComparator();
			
			sortComparator.setConf(conf);
			
			System.out.println("Min index : " + minIndex +  " CRITERIA:" + sortCriteria);
			for (int i= 0 ; i < MAX_RANDOMS_PER_INDEX; i++){
				
				ITuple base1 = new BaseTuple(getConf());
				ITuple base2 = new BaseTuple(getConf());
				ITuple doubleBuffered1 = new Tuple(getConf()); //double buffered
				ITuple doubleBuffered2 = new Tuple(getConf()); //double buffered
				
				ITuple[] tuples = new ITuple[]{base1,base2,doubleBuffered1,doubleBuffered2};
				for (ITuple tuple : tuples){
					fillWithRandom(tuple, minIndex, maxIndex);
				}
				for (int indexTuple1 = 0 ; indexTuple1 < tuples.length; indexTuple1++){
					for (int indexTuple2 = indexTuple1 ; indexTuple2 < tuples.length ; indexTuple2++){
						ITuple tuple1 = tuples[indexTuple1];
						ITuple tuple2 = tuples[indexTuple2];
						assertSameComparison(sortComparator, tuple1,tuple2);
						assertOppositeOrEqualsComparison(sortComparator,tuple1,tuple2);
					}
				}
			} //do you like bracket dance ?
		}
	}
	
	private int compareInBinary(SortComparator comp,ITuple tuple1,ITuple tuple2) throws IOException{
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		tuple1.write(buffer1);
		DataOutputBuffer buffer2 = new DataOutputBuffer();
		tuple2.write(buffer2);
		return comp.compare(buffer1.getData(),0,buffer1.getLength(),buffer2.getData(),0,buffer2.getLength());
	}
	
	private void assertSameComparison(SortComparator comparator,ITuple tuple1,ITuple tuple2) throws IOException{
		
		int compObjects = comparator.compare(tuple1,tuple2);
		int compBinary = compareInBinary(comparator,tuple1,tuple2);
		if (compObjects > 0 && compBinary <=0 || 
				compObjects >= 0 && compBinary <0 ||
				compObjects <=0 && compBinary > 0 ||
				compObjects <0 && compBinary >= 0){
			String error = "Not same comparison : Comp objects:'" + compObjects + "' Comp binary:'" + compBinary + "' for tuples:" +
					"\nTUPLE1:" + tuple1 + "\nTUPLE2:" +tuple2 +  "\nCRITERIA:" + comparator.getSortCriteria();
			//debug purposes
			System.out.println(error);
			//comparator.compare(tuple1,tuple2);
			//compareInBinary(comparator,tuple1,tuple2);
			Assert.fail(error);
		}
	}
	
	private void assertOppositeOrEqualsComparison(SortComparator comp,ITuple tuple1,ITuple tuple2) throws IOException{
		int comp1 = comp.compare(tuple1,tuple2);
		int comp2 = comp.compare(tuple2,tuple1);
		if (comp1 > 0 && comp2 >0 || comp1 <0 && comp2 < 0){
			Assert.fail("Same comparison in OBJECTS: " + comp1 + " , " + comp2 + ".It should be opposite" +"' for tuples:" +
					"\nTUPLE1:" + tuple1 + "\nTUPLE2:" +tuple2 +  "\nCRITERIA:" + comp.getSortCriteria());
		}
		
		comp1 = compareInBinary(comp,tuple1,tuple2);
		comp2 = compareInBinary(comp,tuple2,tuple1);
		if (comp1 > 0 && comp2 >0 || comp1 <0 && comp2 < 0){
			Assert.fail("Same comparison in BINARY: " + comp1 + " , " + comp2 + ".It should be opposite" +"' for tuples:" +
					"\nTUPLE1:" + tuple1 + "\nTUPLE2:" +tuple2 +  "\nCRITERIA:" + comp.getSortCriteria());
		}
	}
	
}
