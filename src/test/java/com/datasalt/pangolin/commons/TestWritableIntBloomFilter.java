package com.datasalt.pangolin.commons;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import com.datasalt.pangolin.io.WritableIntBloomFilter;

public class TestWritableIntBloomFilter {

	@Test
	public void test() {
		test((short)1, 1000);
		test((short)2, 1000);
		test((short)3, 1000);
		test((short)4, 1000);
		test((short)8, 1000);
		test((short)16, 1000);
		test((short)64, 1000);
		test((short)256, 1000);
	}
	
	@Test
	public void testZeroSize() throws IOException {
		WritableIntBloomFilter<Integer> bloomFilter = new WritableIntBloomFilter<Integer>();
		bloomFilter.setBloomFilter((short)0, 0);
		assertEquals(bloomFilter.toByteArray().length, 0);
		
		ByteArrayOutputStream bOs = new ByteArrayOutputStream();
		DataOutputStream dOs = new DataOutputStream(bOs);
		bloomFilter.write(dOs);
		byte[] data = bOs.toByteArray();
		assertEquals(data.length, 6); // 2 + 4
		
		ByteArrayInputStream bIs = new ByteArrayInputStream(data);
		DataInputStream dIs = new DataInputStream(bIs);
		bloomFilter = new WritableIntBloomFilter<Integer>();
		bloomFilter.readFields(dIs);
		
		assertEquals(bloomFilter.toByteArray().length, 0);
		assertEquals(bloomFilter.getFalsePositiveProbability(), 1, 0.00000001);
		for(int i = 0; i < 100; i++) {
			assertTrue(bloomFilter.contains(i));
		}
	}
	
	@Test
	public void testHadoopSerDe() throws IOException {
		WritableIntBloomFilter<Integer> bloomFilter = new WritableIntBloomFilter<Integer>();
		bloomFilter.setBloomFilter((short)256, 1000);
		
		for(int i = 0; i < 1000; i++) {
			bloomFilter.add(i);
		}
		
		ByteArrayOutputStream bOs = new ByteArrayOutputStream();
		DataOutputStream dOs = new DataOutputStream(bOs);
		bloomFilter.write(dOs);
		byte[] data = bOs.toByteArray();
		
		ByteArrayInputStream bIs = new ByteArrayInputStream(data);
		DataInputStream dIs = new DataInputStream(bIs);
		bloomFilter = new WritableIntBloomFilter<Integer>();
		bloomFilter.readFields(dIs);
		
		for(int i = 0; i < 1000; i++) {
			assertTrue(bloomFilter.contains(i));
		}		
	}
	
	public void test(short byteSize, int nElements) {
		WritableIntBloomFilter<Integer> bloomFilter = new WritableIntBloomFilter<Integer>();
		bloomFilter.setBloomFilter(byteSize, nElements);
		
		for(int i = 0; i < 1000; i++) {
			bloomFilter.add(i);
		}

		/*
		 * A bloom filter never gives false negatives
		 */
		for(int i = 0; i < 1000; i++) {
			assertTrue(bloomFilter.contains(i));
		}
		
		/*
		 * Now test serialization
		 */
		byte[] toByteArray = bloomFilter.toByteArray();
		bloomFilter.fromByteArray(toByteArray);
		byte[] againByteArray = bloomFilter.toByteArray();
		assertEquals(toByteArray.length, againByteArray.length);
		for(int i = 0; i < toByteArray.length; i++) {
			assertEquals(toByteArray[i], againByteArray[i]);
		}
		
		for(int i = 0; i < 1000; i++) {
			assertTrue(bloomFilter.contains(i));
		}		
	}	
}
