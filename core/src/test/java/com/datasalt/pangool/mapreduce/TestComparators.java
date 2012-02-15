package com.datasalt.pangool.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.ConfigBuilder;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Criteria.SortElement;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.io.HadoopSerialization;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.io.tuple.ser.PangoolSerialization;
import com.datasalt.pangool.serialization.thrift.ThriftSerialization;

/**
 * This tests either {@link SortComparator} or {@link MyAvroGroupComparator}.It checks that the binary comparison is coherent
 * with the objects comparison.It also checks that the custom comparators are correctly used.
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestComparators extends BaseTest {

	private int MAX_RANDOM_SCHEMAS = 50;
	private HadoopSerialization ser;

	@Test
	public void test() throws CoGrouperException, IOException {
		Random random = new Random();
		Configuration conf = getConf();

		int maxIndex = SCHEMA.getFields().size() - 1;

		Map<String, Class> customComparators = new HashMap<String, Class>();
		customComparators.put("thrift_field", AComparator.class);

		for(int randomSchema = 0; randomSchema < MAX_RANDOM_SCHEMAS; randomSchema++) {
			Schema schema = permuteSchema(SCHEMA);
			SortBy sortCriteria = createRandomSortCriteria(schema, customComparators, maxIndex + 1);
			String[] groupFields = getFirstFields(sortCriteria, random.nextInt(sortCriteria.getElements().size()));

			ITuple[] tuples = new ITuple[] { new Tuple(schema), new Tuple(schema) };
			for(ITuple tuple: tuples) {
				fillTuple(false, tuple, 0, maxIndex);
			}
			
			for(int minIndex = maxIndex; minIndex >= 0; minIndex--) {
				ConfigBuilder builder = new ConfigBuilder();
				builder.addSourceSchema(schema);
				builder.setGroupByFields(groupFields);
				builder.setOrderBy(sortCriteria);
				
				CoGrouperConfig grouperConf = builder.buildConf();
				CoGrouperConfig.set(grouperConf, conf);
				
				// grouperConf has changed -> we need a new Serialization object
				ser = new HadoopSerialization(conf);
				
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

	private int compareInBinary1(SortComparator comp, ITuple tuple1, ITuple tuple2) throws IOException {
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.ser(new DatumWrapper(tuple1), buffer1);
		
		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(new DatumWrapper(tuple2), buffer2);
		
		return comp.compare(buffer1.getData(), 0, buffer1.getLength(), buffer2.getData(), 0, buffer2.getLength());
	}
	
	/**
	 * 
	 * Checks that the binary comparison matches the comparison by objects.
	 * 
	 */
	private void assertSameComparison(String alias, SortComparator comparator, ITuple tuple1, ITuple tuple2)
	    throws IOException {

		int compObjects = comparator.compare(tuple1, tuple2);
		int compBinary = compareInBinary1(comparator, tuple1, tuple2);
		if(compObjects > 0 && compBinary <= 0 || compObjects >= 0 && compBinary < 0 || compObjects <= 0 && compBinary > 0
		    || compObjects < 0 && compBinary >= 0) {

			String error = alias + ",Not same comparison : Comp objects:'" + compObjects + "' Comp binary:'" + compBinary
			    + "' for tuples:" + "\nTUPLE1:" + tuple1 + "\nTUPLE2:" + tuple2 + 
			    "\nCOMMON ORDER:"  + comparator.getConfig().getCommonSortBy() + 
			    "\nSECONDARY ORER:" + comparator.getConfig().getSecondarySortBys();
			Assert.fail(error);
		}
	}

	/**
	 * Checks that comp(tuple1,tuple2) is -comp(tuple2,tuple1)
	 */
	private void assertOppositeOrEqualsComparison(SortComparator comp, ITuple tuple1, ITuple tuple2) throws IOException {
		int comp1 = comp.compare(tuple1, tuple2);
		int comp2 = comp.compare(tuple2, tuple1);
		if(comp1 > 0 && comp2 > 0 || comp1 < 0 && comp2 < 0) {
			Assert.fail("Same comparison in OBJECTS: " + comp1 + " , " + comp2 + ".It should be opposite" + "' for tuples:"
			    + "\nTUPLE1:" + tuple1 + 
			    "\nTUPLE2:" + tuple2 + 
			    "\nCOMMON ORDER:"   + comp.getConfig().getCommonSortBy() + 
			    "\nSECONDARY ORDER:"   + comp.getConfig().getSecondarySortBys());
		}

		comp1 = compareInBinary1(comp, tuple1, tuple2);
		comp2 = compareInBinary1(comp, tuple2, tuple1);
		if(comp1 > 0 && comp2 > 0 || comp1 < 0 && comp2 < 0) {
			Assert.fail("Same comparison in BINARY: " + comp1 + " , " + comp2 + ".It should be opposite" + "' for tuples:"
			    + "\nTUPLE1:" + tuple1 + "\nTUPLE2:" + tuple2 + 
			    "\nCOMMON CRITERIA:"  + comp.getConfig().getCommonSortBy() + 
			    "\nSECONDARY ORDER:"   + comp.getConfig().getSecondarySortBys());
		}
	}

	/**
	 * Custom comparator
	 * 
	 * @author epalace
	 * 
	 */
	private static class AComparator implements RawComparator<com.datasalt.pangolin.thrift.test.A>, Configurable {

		private Configuration conf;
		private HadoopSerialization ser;

		private A cachedInstance1 = new A();
		private A cachedInstance2 = new A();

		@Override
		public int compare(A o1, A o2) {
			if(o1 != null && o2 == null) {
				return 1;
			} else if(o1 == null && o2 != null) {
				return -1;
			} else if(o1 == null && o2 == null) {
				return 0;
			} else {
				return o1.compareTo(o2);
			}
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			A a1, a2;
			try {
				a1 = (l1 == 0) ? (A) null : (A) ser.deser(cachedInstance1, b1, s1, l1);
				a2 = (l2 == 0) ? (A) null : (A) ser.deser(cachedInstance2, b2, s2, l2);
				return compare(a1, a2);
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void setConf(Configuration conf) {
			if(conf != null) {
				this.conf = conf;
				try {
					this.ser = new HadoopSerialization(conf);
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

	/**
	 * Creates a copy of the schema with the fields shuffled.
	 */
	private static Schema permuteSchema(Schema schema) {
		List<Field> fields = schema.getFields();
		List<Field> permutedFields = new ArrayList<Field>(fields);
		Collections.shuffle(permutedFields);
		return new Schema("new_schema",permutedFields);
	}

	/**
	 * 
	 * Creates a random sort criteria based in the specified schema.
	 * @throws CoGrouperException 
	 */
	private static SortBy createRandomSortCriteria(Schema schema, Map<String, Class> customComparators,
	    int numFields) throws CoGrouperException {
			Random random = new Random();
			List<SortElement> builder = new ArrayList<SortElement>();
			for(int i = 0; i < numFields; i++) {
				Field field = schema.getField(i);
				builder.add(new SortElement(field.getName(), random.nextBoolean() ? Order.ASC : Order.DESC,
				    customComparators.get(field.getName())));
			}
			return new SortBy(builder);
	}

	private String[] getFirstFields(SortBy sortCriteria, int numFields) {
		String[] result = new String[numFields];
		for(int i = 0; i < numFields; i++) {
			SortElement element = sortCriteria.getElements().get(i);
			result[i] = element.getName();
		}
		return result;
	}
}