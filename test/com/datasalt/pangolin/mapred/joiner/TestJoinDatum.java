package com.datasalt.pangolin.mapred.joiner;

import static org.junit.Assert.assertEquals;

import org.apache.thrift.TException;
import org.junit.Test;

import com.datasalt.pangolin.mapred.joiner.JoinDatum;
import com.datasalt.pangolin.mapred.joiner.JoinDatum.Source;


public class TestJoinDatum {

	@Test
	public void test(){}
	
	//TODO
//	@Test
//	public void test() throws InstantiationException, IllegalAccessException, TException {
//		ChannelProfileToFetch channel  = new ChannelProfileToFetch();
//		channel.setUid("blabla");
//		JoinDatum<ChannelProfileToFetch> datum = new JoinDatum<ChannelProfileToFetch>(null,channel);
//		//JoinDatum datum = JoinDatum.class.newInstance();
//		assertEquals(datum.getSource(),null);
//		
//		datum.setSource(Source.OLD);
//		assertEquals(datum.getSource(),Source.OLD);
//	}
}
