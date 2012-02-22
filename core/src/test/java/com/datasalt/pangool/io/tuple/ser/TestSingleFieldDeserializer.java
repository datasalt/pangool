package com.datasalt.pangool.io.tuple.ser;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import cern.colt.Arrays;

import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.ConfigBuilder;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.io.BaseComparator;
import com.datasalt.pangool.io.HadoopSerialization;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.mapreduce.SortComparator;
import com.datasalt.pangool.test.AbstractBaseTest;

/**
 * TODO: Build a proper test. 
 */
@SuppressWarnings("serial")
public class TestSingleFieldDeserializer extends AbstractBaseTest implements Serializable {

	@Test
	public void testThrift() throws IOException, CoGrouperException {
		Configuration conf = getConf();
		
		ArrayList<Field> fields = new ArrayList<Field> ();
		fields.add(new Field("a", A.class));
		Schema schema = new Schema("schema", fields);

		Tuple tuple1 = new Tuple(schema);
		final A a = new A("hola", "colega");
		tuple1.set("a", a);

		Tuple tuple2 = new Tuple(schema);
		tuple2.set("a", null);
		
		ConfigBuilder builder = new ConfigBuilder();
		builder.addSourceSchema(schema);
		builder.setGroupByFields("a");
		builder.setOrderBy(new SortBy().add("a", Order.ASC, new BaseComparator<A>(A.class) {

			@Override
      public int compare(A o1, A o2) {
				assertEquals(a, o1);
				assertTrue(o2 == null);
				
				return 1;
      }
			
		}));		
		
		CoGrouperConfig grouperConf = builder.buildConf();
		CoGrouperConfig.set(grouperConf, conf);
		
		HadoopSerialization ser = new HadoopSerialization(conf);
	
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.ser(new DatumWrapper(tuple1), buffer1);

		SingleFieldDeserializer fieldDeser = new SingleFieldDeserializer(conf, grouperConf, A.class);
		A otherA = (A) fieldDeser.deserialize(buffer1.getData(), 0);
		assertEquals(a, otherA);

		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(new DatumWrapper(tuple2), buffer2);
		
		SortComparator sortComparator = new SortComparator();
		sortComparator.setConf(conf);

		System.out.println("buff1: " + Arrays.toString(buffer1.getData()));
		System.out.println("buff2: " + Arrays.toString(buffer2.getData()));
		
		sortComparator.compare(buffer1.getData(), 0, buffer1.size(), buffer2.getData(), 0, buffer2.size());
			
	}

}
