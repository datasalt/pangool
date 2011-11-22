package com.datasalt.pangolin.grouper;

import java.io.IOException;

import com.datasalt.pangolin.commons.test.AbstractHadoopTestLibrary;
import com.datasalt.pangolin.grouper.io.Tuple;
import com.datasalt.pangolin.grouper.io.TupleSortComparator;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.thrift.test.A;


import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTupleComparator extends AbstractHadoopTestLibrary{
	
	@Test
	public void test() throws GrouperException, IOException{
		
		TupleSortComparator comp = new TupleSortComparator();
		A a = new A();
		Schema schema = Schema.parse("name:string,age:int,risas:" + a.getClass().getName());
		comp.setSchema(schema);
		SortCriteria sortCriteria = SortCriteria.parse("name desc,age asc,risas desc");
		comp.setSortCriteria(sortCriteria);
		a.setId("id_guapisimo");
		a.setUrl("www.gerrerreg.com");
		Tuple tuple1 = new Tuple();
		tuple1.setSchema(schema);
		tuple1.setSerialization(getSer());
		Tuple tuple2 = new Tuple();
		tuple2.setSchema(schema);
		tuple2.setSerialization(getSer());
		tuple2.setField("risas",a);
		
		Serialization ser = getSer();
		
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.ser(tuple1, buffer1);
		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(tuple2,buffer2);
		
		int result = comp.compare(buffer1.getData(),0,buffer1.getLength(),buffer2.getData(),0,buffer2.getLength());
		
		//TODO 
		//assertEquals(0,result);
		
		//TODO improve test
		//fail();
		
	}

}
