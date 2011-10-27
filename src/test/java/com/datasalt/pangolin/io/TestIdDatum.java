package com.datasalt.pangolin.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.datasalt.pangolin.thrift.test.A;

import org.apache.thrift.TException;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.BaseTest;
import com.datasalt.pangolin.io.IdDatumBase;
import com.datasalt.pangolin.io.Serialization;

import static org.junit.Assert.*;

public class TestIdDatum extends BaseTest {

	@Test
	public void testSerialization() throws TException, IOException {
		Serialization ser = getSer();
		A a1 = new A();
		a1.setId("1");
		a1.setUrl("1");		

		
		IdDatumBase datum1 = new IdDatumBase(1, ser.ser(a1));

		assertEquals(1, datum1.getIdentifier());
		
		A a = new A();
		ser.deser(a, datum1.getItem1());
		assertEquals(a.getId(),  "1");
		assertEquals(a.getUrl(), "1");
		
		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		byte[] bytes = oS.toByteArray();
		IdDatumBase datum2 = new IdDatumBase();
		ByteArrayInputStream iS = new ByteArrayInputStream(bytes);
		datum2.readFields(new DataInputStream(iS));
		
		assertEquals(1, datum2.getIdentifier());
		
		a = new A();
		ser.deser(a, datum2.getItem1());
		assertEquals(a.getId(),  "1");
		assertEquals(a.getUrl(), "1");
				
		assertEquals(datum1.hashCode(), datum2.hashCode());
		assertEquals(datum1, datum2);
		assertEquals(0, datum1.compareTo(datum2));
	}
	
	
	@Test
	public void testItemsDifferenciation() throws TException, IOException{
		Serialization ser = getSer();
		A a1 = new A();
		a1.setId("1");
		a1.setUrl("1");		

		A a11 = new A();
		a11.setId("11");
		a11.setUrl("11");

		// Different 
		
		IdDatumBase datum1 = new IdDatumBase((short) 1, ser.ser(a1));
		IdDatumBase datum2 = new IdDatumBase((short) 1, ser.ser(a11));
		
		int normalCmp = datum1.compareTo(datum2);
		assertTrue(0 != normalCmp);

		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		byte[] d1 = oS.toByteArray();
		
		oS = new ByteArrayOutputStream();
		datum2.write(new DataOutputStream(oS));
		byte[] d2 = oS.toByteArray();
		
		int rawCmp = new IdDatumBase.Comparator().compare(d1, 0, d1.length, d2, 0, d2.length);
		assertTrue(0 != rawCmp);
		assertTrue(rawCmp < 0 && normalCmp < 0);
		
		// Equals
		
		datum1 = new IdDatumBase((short) 1, ser.ser(a11));
		datum2 = new IdDatumBase((short) 1, ser.ser(a11));
		
		normalCmp = datum1.compareTo(datum2);
		assertTrue(0 == normalCmp);

		oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		d1 = oS.toByteArray();
		
		oS = new ByteArrayOutputStream();
		datum2.write(new DataOutputStream(oS));
		d2 = oS.toByteArray();
		
		rawCmp = new IdDatumBase.Comparator().compare(d1, 0, d1.length, d2, 0, d2.length);
		assertTrue(0 == rawCmp);

	}
	
	
}