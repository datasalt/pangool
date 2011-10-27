package com.datasalt.pangolin.mapred.joiner;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.datasalt.pangolin.thrift.test.A;

import org.apache.thrift.TException;
import org.junit.Test;

import com.datasalt.pangolin.commons.ThriftUtils;
import com.datasalt.pangolin.mapred.joiner.MultiJoinDatum;

public class TestMultiJoinDatum {

	@Test
	public void test() throws TException, IOException {
		MultiJoinDatum<A> datum = new MultiJoinDatum<A>();
		A a = new A();
		a.setId("id1");
		a.setUrl("url");
		datum.setChannelId((byte)2);
		datum.setDatum(ThriftUtils.getSerializer().serialize(a));
		
		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum.write(new DataOutputStream(oS));
		byte[] bytes = oS.toByteArray();
		datum = new MultiJoinDatum<A>();
		ByteArrayInputStream iS = new ByteArrayInputStream(bytes);
		datum.readFields(new DataInputStream(iS));
		
		a = new A();
		byte[] newArray = new byte[datum.getDatum().getLength()];
		System.arraycopy(datum.getDatum().getBytes(),0,newArray,0,datum.getDatum().getLength());
		ThriftUtils.getDeserializer().deserialize(a,newArray);
		assertEquals(a.getId(),  "id1");
		assertEquals(a.getUrl(), "url");
		assertEquals(datum.getChannelId(), 2);
	}
}
