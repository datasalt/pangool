/**
 * Copyright [2012] [Datasalt Systems S.L.]
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
package com.datasalt.pangool.tuplemr.mapred;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.datasalt.pangool.tuplemr.*;
import com.datasalt.pangool.utils.InstancesDistributor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.thrift.test.A;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;

/**
 * This tests either {@link SortComparator} or {@link GroupComparator}.It checks
 * that the binary comparison is coherent with the objects comparison.It also
 * checks that the custom comparators are correctly used.
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestComparators extends ComparatorsBaseTest {

	private int MAX_RANDOM_SCHEMAS = 25;
	private HadoopSerialization ser;
	static Random random = new Random(1);

	@Test
	public void testObjectComparison() throws TupleMRException, IOException {
		SortComparator c = new SortComparator();
		setConf(c);

		// source 1
		Assert.assertEquals(0, c.compare(getTuple1(true, 10, "a"), getTuple1(true, 10, "a")));
		assertNegative(c, getTuple1(false, 10, "a"), getTuple1(true, 10, "a"));
		assertPositive(c, getTuple1(true, 10, "a"), getTuple1(false, 10, "a"));
		assertPositive(c, getTuple1(true, 1, "a"), getTuple1(true, 10, "a"));
		assertNegative(c, getTuple1(true, 10, "a"), getTuple1(true, 1, "a"));
		assertNegative(c, getTuple1(true, 10, "b"), getTuple1(true, 10, "a"));
		assertPositive(c, getTuple1(true, 10, "a"), getTuple1(true, 10, "b"));

		// // Different sources comparing
		assertPositive(c, getTuple1(true, 10, ""), getTuple2(true, 10, -1));
		assertNegative(c, getTuple2(true, 10, -1), getTuple1(true, 10, ""));
		//
		// source 2
		Assert.assertEquals(0, c.compare(getTuple2(true, 10, 0), getTuple2(true, 10, 0)));
		assertNegative(c, getTuple2(false, 10, 0), getTuple2(true, 10, 0));
		assertPositive(c, getTuple2(true, 10, 0), getTuple2(false, 10, 0));
		assertPositive(c, getTuple2(true, 1, 0), getTuple2(true, 10, 0));
		assertNegative(c, getTuple2(true, 10, 0), getTuple2(true, 1, 0));
		assertPositive(c, getTuple2(true, 10, 0), getTuple2(true, 10, 10));
		assertNegative(c, getTuple2(true, 10, 10), getTuple2(true, 10, 0));
	}

  @Test
  public void testCrossValidationOneSchema() throws TupleMRException, IOException {
    testCrossValidationOneSchema(false);
  }

  /**
   * Comparator validation allowing nulls.
   */
  @Test
  public void testCrossValidationOneSchemaWithNulls() throws TupleMRException, IOException {
    testCrossValidationOneSchema(true);
  }

	public void testCrossValidationOneSchema(boolean withNulls) throws TupleMRException, IOException {
		Configuration conf = getConf();

		int maxIndex = SCHEMA.getFields().size() - 1;

		for(int randomSchema = 0; randomSchema < MAX_RANDOM_SCHEMAS; randomSchema++) {
			Schema schema = permuteSchema(SCHEMA);
      if (withNulls) {
        schema = decorateWithNullables(schema);
      }
			OrderBy sortCriteria = createRandomSortCriteria(schema, maxIndex + 1);
			// TODO could we get empty group fields ??
			String[] groupFields = getFirstFields(sortCriteria,
			    1 + random.nextInt(sortCriteria.getElements().size() - 1));
			ITuple[] tuples = new ITuple[] { new Tuple(schema), new Tuple(schema) };
			for(ITuple tuple : tuples) {
				fillTuple(false, tuple, 0, maxIndex);
			}

			for(int minIndex = maxIndex; minIndex >= 0; minIndex--) {
				conf = createConf();
				/* trick for speeding up the tests */
				InstancesDistributor.removeFromTemporalFolder(conf, "comparator.dat");
				TupleMRConfigBuilder builder = new TupleMRConfigBuilder();
				builder.addIntermediateSchema(schema);
				builder.setGroupByFields(groupFields);
				builder.setOrderBy(sortCriteria);

				TupleMRConfig tupleMRConf = builder.buildConf();
				TupleMRConfig.set(tupleMRConf, conf);

				// tupleMRConf has changed -> we need a new Serialization object
				try{
				ser = new HadoopSerialization(conf);
				} catch(Exception e){
					throw new IOException(e);
				}

				SortComparator sortComparator = new SortComparator();
				GroupComparator groupComparator = new GroupComparator();

				sortComparator.setConf(conf);
				groupComparator.setConf(conf);

				for(ITuple tuple : tuples) {
					fillTuple(true, tuple, minIndex, maxIndex);
				}
				for(int indexTuple1 = 0; indexTuple1 < tuples.length; indexTuple1++) {
					for(int indexTuple2 = indexTuple1 + 1; indexTuple2 < tuples.length; indexTuple2++) {
						ITuple tuple1 = tuples[indexTuple1];
						ITuple tuple2 = tuples[indexTuple2];
						assertSameComparison("Sort comparator", sortComparator, tuple1, tuple2);
						assertOppositeOrEqualsComparison(sortComparator, tuple1, tuple2);
						assertSameComparison("Group comparator", groupComparator, tuple1, tuple2);
						assertOppositeOrEqualsComparison(groupComparator, tuple1, tuple2);
					}
				}
			}
		}
	}

	private int compareInBinary1(SortComparator comp, ITuple tuple1, ITuple tuple2)
	    throws IOException {
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.ser(new DatumWrapper(tuple1), buffer1);

		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(new DatumWrapper(tuple2), buffer2);

		return comp.compare(buffer1.getData(), 0, buffer1.getLength(), buffer2.getData(), 0,
		    buffer2.getLength());
	}

	/**
	 * 
	 * Checks that the binary comparison matches the comparison by objects.
	 * 
	 */
	private void assertSameComparison(String alias, SortComparator comparator,
	    ITuple tuple1, ITuple tuple2) throws IOException {
		boolean DEBUG = true;
		int compObjects = comparator.compare(tuple1, tuple2);
		int compBinary = compareInBinary1(comparator, tuple1, tuple2);
		if(compObjects > 0 && compBinary <= 0 || compObjects >= 0 && compBinary < 0
		    || compObjects <= 0 && compBinary > 0 || compObjects < 0 && compBinary >= 0) {

			String error = alias + ",Not same comparison : Comp objects:'" + compObjects
			    + "' Comp binary:'" + compBinary + "' for tuples:" + "\nTUPLE1:" + tuple1
			    + "\nTUPLE2:" + tuple2 + "\nCONFIG:" + comparator.getConfig();
			if(DEBUG) {
				System.err.println(error);
				comparator.compare(tuple1, tuple2);
				compareInBinary1(comparator, tuple1, tuple2);
			}

			Assert.fail(error);
		}
	}

	/**
	 * Checks that comp(tuple1,tuple2) is -comp(tuple2,tuple1)
	 */
	private void assertOppositeOrEqualsComparison(SortComparator comp, ITuple tuple1,
	    ITuple tuple2) throws IOException {
		int comp1 = comp.compare(tuple1, tuple2);
		int comp2 = comp.compare(tuple2, tuple1);
		if(comp1 > 0 && comp2 > 0 || comp1 < 0 && comp2 < 0) {
			Assert.fail("Same comparison in OBJECTS: " + comp1 + " , " + comp2 + "."
			    + "It should be opposite" + "' for tuples:" + "\nTUPLE1:" + tuple1
			    + "\nTUPLE2:" + tuple2 + "\nCONFIG:" + comp.getConfig());
		}

		comp1 = compareInBinary1(comp, tuple1, tuple2);
		comp2 = compareInBinary1(comp, tuple2, tuple1);
		if(comp1 > 0 && comp2 > 0 || comp1 < 0 && comp2 < 0) {
			Assert.fail("Same comparison in BINARY: " + comp1 + " , " + comp2 + "."
			    + "It should be opposite" + "' for tuples:" + "\nTUPLE1:" + tuple1
			    + "\nTUPLE2:" + tuple2 + "\nCONFIG:" + comp.getConfig());

		}
	}

	/**
	 * Creates a copy of the schema with the fields shuffled.
	 */
	protected static Schema permuteSchema(Schema schema) {
		List<Field> fields = schema.getFields();
		List<Field> permutedFields = new ArrayList<Field>(fields);
		Collections.shuffle(permutedFields);
		return new Schema("new_schema", permutedFields);
	}

	@SuppressWarnings("serial")
  public static class DummyComparator implements RawComparator,Serializable {
		public DummyComparator(){}
		@Override
		public int compare(Object ob1, Object ob2) {
			return 0;
		}

		@Override
		public int compare(byte[] b1, int offset1, int length1, byte[] b2, int o2,
				int l2) {
			return 0;
		}
	}

	/**
	 * 
	 * Creates a random sort criteria based in the specified schema.
	 * 
	 * @throws TupleMRException
	 */
	protected static OrderBy createRandomSortCriteria(Schema schema, int numFields)
	    throws TupleMRException {
		List<SortElement> builder = new ArrayList<SortElement>();
		for(int i = 0; i < numFields; i++) {
			Field field = schema.getField(i);
			if (field.getType() == Type.OBJECT && field.getName().equals("my_avro") && random.nextBoolean()) {
				// With custom comparator
				builder.add(new SortElement(field.getName(), random.nextBoolean() ? Order.ASC
				    : Order.DESC,
            random.nextBoolean() ? Criteria.NullOrder.NULLS_FIRST : Criteria.NullOrder.NULLS_LAST,
            new DummyComparator()));
			} else {
				// Without custom comparator
				builder.add(new SortElement(field.getName(), random.nextBoolean() ? Order.ASC
				    : Order.DESC, random.nextBoolean() ? Criteria.NullOrder.NULLS_FIRST : Criteria.NullOrder.NULLS_LAST));
			}
		}
		return new OrderBy(builder);
	}

	protected static String[] getFirstFields(OrderBy sortCriteria, int numFields) {
		String[] result = new String[numFields];
		for(int i = 0; i < numFields; i++) {
			SortElement element = sortCriteria.getElements().get(i);
			result[i] = element.getName();
		}
		return result;
	}

	public static final class MyThriftComparator implements RawComparator<A> {

		private HadoopSerialization hadoopSer;
		A a1=new A(),a2=new A();
		public MyThriftComparator(Configuration conf){
			try {
				hadoopSer = new HadoopSerialization(conf);
			} catch (IOException e) {
				throw new PangoolRuntimeException(e);
			}
		}

		@Override
		public int compare(byte[] b1, int o1, int l1, byte[] b2, int o2,
				int l2) {
			try {
				a1 = hadoopSer.deser(a1, b1, o1, l1);
				a2 = hadoopSer.deser(a2, b2, o2, l2);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return compare(a1,a2);
		}

		@Override
		public int compare(A o1, A o2) {
			return o2.getId().compareTo(o1.getId());
		}
		
	}
	
	@Test
	public void testCompareObjects() throws IOException {
		// Testing behaviour of the method with non Object types.
		// Object behaviour not tested here.
		SortComparator sortComparator = new SortComparator();
		MyThriftComparator thriftComp = new MyThriftComparator(getConf());
		assertEquals(1, sortComparator.compareObjects(new A("1",null),new A("2",null), thriftComp, Type.OBJECT,null));
		assertEquals(0, sortComparator.compareObjects(new A("2",null),new A("2",null),thriftComp, Type.OBJECT,null));
		assertEquals(-1, sortComparator.compareObjects(new A("3",null),new A("2",null), thriftComp, Type.OBJECT,null));

		assertEquals(1, sortComparator.compareObjects(new A("1",null), new A("2",null), thriftComp,Type.OBJECT,null));
		assertEquals(0, sortComparator.compareObjects(new A("2",null), new A("2", null),thriftComp,Type.OBJECT,null));
		assertEquals(-1, sortComparator.compareObjects(new A("3",null), new A("2",null), thriftComp,Type.OBJECT,null));
	}

}