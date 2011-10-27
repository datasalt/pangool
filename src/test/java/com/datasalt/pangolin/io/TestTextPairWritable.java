package com.datasalt.pangolin.io;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.datasalt.pangolin.commons.WritableUtils;

import org.junit.Test;

import com.datasalt.pangolin.io.TextPairWritable;

public class TestTextPairWritable {

	public static final String LARGE_STRING_1="abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcab1";
	public static final String LARGE_STRING_2="abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcab2";
	
	@Test
	public void testComparator(){
		
		TextPairWritable t1 = new TextPairWritable();
		TextPairWritable t2 = new TextPairWritable();
		
		t1.setFirst(LARGE_STRING_1);
		t2.setFirst(LARGE_STRING_1);
		t1.setSecond(LARGE_STRING_1);
		t2.setSecond(LARGE_STRING_1);
		TextPairWritable.Comparator comparator = new TextPairWritable.Comparator();
		assertTrue(comparator.compare(t1,t2) == 0);
		
		t2.setSecond(LARGE_STRING_2);
		assertTrue(comparator.compare(t1,t2) < 0);
		
		t2.setSecond(LARGE_STRING_1);
		t1.setSecond(LARGE_STRING_2);
		assertTrue(comparator.compare(t1,t2) > 0);
		
		t1.setFirst(LARGE_STRING_1);
		t2.setFirst(LARGE_STRING_2);
		assertTrue(comparator.compare(t1, t2) < 0);
		
		t1.setFirst(LARGE_STRING_2);
		t2.setFirst(LARGE_STRING_1);
		assertTrue(comparator.compare(t1, t2) > 0);
		
	}
	
	public void testBinaryComparator() throws IOException{
		TextPairWritable t1 = new TextPairWritable();
		TextPairWritable t2 = new TextPairWritable();
		
		t1.setFirst(LARGE_STRING_1);
		t2.setFirst(LARGE_STRING_1);
		t1.setSecond(LARGE_STRING_1);
		t2.setSecond(LARGE_STRING_1);
		TextPairWritable.Comparator comparator = new TextPairWritable.Comparator();
		
    byte[] array1 = WritableUtils.serialize(t1);
    byte[] array2 = WritableUtils.serialize(t2);
    
    assertTrue(comparator.compare(array1,0,array1.length, array2,0,array2.length) == 0);
    
    t1.setSecond(LARGE_STRING_2);
    array1 = WritableUtils.serialize(t1);
    array2 = WritableUtils.serialize(t2);
    assertTrue(comparator.compare(array1,0,array1.length, array2,0,array2.length) > 0);
    
    t1.setSecond(LARGE_STRING_1);
    t2.setSecond(LARGE_STRING_2);
    array1 = WritableUtils.serialize(t1);
    array2 = WritableUtils.serialize(t2);
    assertTrue(comparator.compare(array1,0,array1.length, array2,0,array2.length) < 0);
    
	}
	
	public void testSubstringComparator() throws IOException{
		TextPairWritable t1 = new TextPairWritable();
		TextPairWritable t2 = new TextPairWritable();
		
		t1.setFirst(LARGE_STRING_1);
		t2.setFirst(LARGE_STRING_1);
		t1.setSecond(LARGE_STRING_1);
		t2.setSecond(LARGE_STRING_1);
		TextPairWritable.FirstStringComparator comparator = new TextPairWritable.FirstStringComparator();
		
    byte[] array1 = WritableUtils.serialize(t1);
    byte[] array2 = WritableUtils.serialize(t2);
    
    assertTrue(comparator.compare(array1,0,array1.length, array2,0,array2.length) == 0);
    
    t1.setSecond(LARGE_STRING_2);
    array1 = WritableUtils.serialize(t1);
    array2 = WritableUtils.serialize(t2);
    assertTrue(comparator.compare(array1,0,array1.length, array2,0,array2.length) == 0);
    
    t1.setSecond(LARGE_STRING_1);
    t2.setSecond(LARGE_STRING_2);
    array1 = WritableUtils.serialize(t1);
    array2 = WritableUtils.serialize(t2);
    assertTrue(comparator.compare(array1,0,array1.length, array2,0,array2.length) == 0);
    
    t1.setFirst(LARGE_STRING_2);
    array1 = WritableUtils.serialize(t1);
    array2 = WritableUtils.serialize(t2);
    assertTrue(comparator.compare(array1,0,array1.length, array2,0,array2.length) > 0);
    
    
    
	}
	
	
}
