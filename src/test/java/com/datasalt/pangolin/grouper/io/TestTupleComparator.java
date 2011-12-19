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

package com.datasalt.pangolin.grouper.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.junit.Test;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.SortCriteria;
import com.datasalt.pangolin.grouper.io.TupleImpl;
import com.datasalt.pangolin.grouper.io.TupleSortComparator;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.thrift.test.A;


public class TestTupleComparator extends AbstractHadoopTestLibrary{
	
	private static class Comparator1 implements RawComparator<com.datasalt.pangolin.thrift.test.A>,Configurable {

		private Configuration conf;
		private Serialization ser;
		
		private A a1=new A();
		private A a2=new A();
		
		public Comparator1(){
		}

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
	
	
	@Test
	public void test() throws GrouperException, IOException{
		
		
		A a1 = new A();
		a1.setId("1");
		a1.setUrl("1");
		
		A a2 = new A();
		a2.setId("1");
		a2.setUrl("2");
		
		Configuration conf=getConf();
		FieldsDescription schema = FieldsDescription.parse("name:string,age:int,risas:" + a1.getClass().getName());
		FieldsDescription.setInConfig(schema, conf);
		SortCriteria sortCriteria = SortCriteria.parse("name desc,age asc,risas using " + Comparator1.class.getName() +  " desc");
		SortCriteria.setInConfig(sortCriteria, conf);
		
		TupleSortComparator comp = new TupleSortComparator();
		comp.setConf(conf);
		
		TupleImpl tuple1 = new TupleImpl(schema);
		//tuple1.setSchema(schema);
		tuple1.setSerialization(getSer());
		tuple1.setThriftObject("risas",a1);
		
		TupleImpl tuple2 = new TupleImpl(schema);
		tuple2.setSchema(schema);
		tuple2.setSerialization(getSer());
		tuple2.setThriftObject("risas",a1);
		
		assertEquals(0,compareInBinary(comp,tuple1,tuple2));
	}
	
	private int compareInBinary(RawComparator<?> comp,TupleImpl tuple1,TupleImpl tuple2) throws IOException{
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		tuple1.write(buffer1);
		DataOutputBuffer buffer2 = new DataOutputBuffer();
		tuple2.write(buffer2);
		return comp.compare(buffer1.getData(),0,buffer1.getLength(),buffer2.getData(),0,buffer2.getLength());
	}
}
