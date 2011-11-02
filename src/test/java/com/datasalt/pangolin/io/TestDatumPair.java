package com.datasalt.pangolin.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.datasalt.pangolin.thrift.test.A;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.PangolinBaseTest;
import com.datasalt.pangolin.io.DatumPairBase;
import com.datasalt.pangolin.io.Serialization;

import static org.junit.Assert.*;

public class TestDatumPair extends PangolinBaseTest {

	Serialization ser; 
	
	@Before
	public void startUp() throws IOException {
		ser = getSer();
	}
	
	@Test
	public void testSerialization() throws TException, IOException {
		
		A a1 = new A();
		a1.setId( "1");
		a1.setUrl("1");		

		A a11 = new A();
		a11.setId("11");
		a11.setUrl("11");
		
		DatumPairBase datum1 = new DatumPairBase(ser.ser(a1), ser.ser(a11));

		A a = new A();
		ser.deser(a, datum1.getItem1());
		assertEquals(a.getId(),  "1");
		assertEquals(a.getUrl(), "1");
		
		a = new A();
		ser.deser(a, datum1.getItem2());
		assertEquals(a.getId(), "11");
		assertEquals(a.getUrl(), "11");

		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		byte[] bytes = oS.toByteArray();
		DatumPairBase datum2 = new DatumPairBase();
		ByteArrayInputStream iS = new ByteArrayInputStream(bytes);
		datum2.readFields(new DataInputStream(iS));
		
		a = new A();
		ser.deser(a, datum2.getItem1());
		assertEquals(a.getId(),  "1");
		assertEquals(a.getUrl(), "1");
		
		a = new A();
		ser.deser(a, datum2.getItem2());
		assertEquals(a.getId(), "11");
		assertEquals(a.getUrl(), "11");
		
		assertEquals(datum1.hashCode(), datum2.hashCode());
		assertEquals(datum1, datum2);
		assertEquals(0, datum1.compareTo(datum2));
	}
	
	
	@Test
	public void testItemsDifferenciation() throws TException, IOException{
		A a1 = new A();
		a1.setId( "1");
		a1.setUrl("1");		

		A a11 = new A();
		a11.setId("11");
		a11.setUrl("11");
		
		DatumPairBase datum1 = new DatumPairBase(ser.ser(a1), ser.ser(a11));
		DatumPairBase datum2 = new DatumPairBase(ser.ser(a11), ser.ser(a1));
		
		int normalCmp = datum1.compareTo(datum2);
		assertTrue(0 != normalCmp);

		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		byte[] d1 = oS.toByteArray();
		
		oS = new ByteArrayOutputStream();
		datum2.write(new DataOutputStream(oS));
		byte[] d2 = oS.toByteArray();
		
		int rawCmp = new DatumPairBase.ComparatorWithNoOrder().compare(d1, 0, d1.length, d2, 0, d2.length);
		assertTrue(0 != rawCmp);
		assertTrue(rawCmp < 0 && normalCmp < 0);
		
	}
	

}