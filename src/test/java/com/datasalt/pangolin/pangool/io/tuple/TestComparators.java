package com.datasalt.pangolin.pangool.io.tuple;

import java.io.IOException;
import java.lang.reflect.Method;
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
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.BaseTuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.serialization.TupleSerialization;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangolin.pangool.BaseTest;
import com.datasalt.pangolin.pangool.CoGrouperException;
import com.datasalt.pangolin.pangool.PangoolConfig;
import com.datasalt.pangolin.pangool.PangoolConfigBuilder;
import com.datasalt.pangolin.pangool.Schema;
import com.datasalt.pangolin.pangool.Schema.Field;
import com.datasalt.pangolin.pangool.SchemaBuilder;
import com.datasalt.pangolin.pangool.SortCriteria;
import com.datasalt.pangolin.pangool.SortCriteria.SortElement;
import com.datasalt.pangolin.pangool.SortCriteria.SortOrder;
import com.datasalt.pangolin.pangool.Sorting;
import com.datasalt.pangolin.pangool.SortingBuilder;
import com.datasalt.pangolin.thrift.test.A;

/**
 * This tests either {@link SortComparator} or {@link GroupComparator}.It checks that the binary comparison is coherent
 * with the objects comparison.It also checks that the custom comparators are correctly used.
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestComparators extends BaseTest {

	int MAX_RANDOM_SCHEMAS = 200;
	int MAX_RANDOMS_PER_INDEX = 20;

	@Ignore
	@Test
	public void test() throws CoGrouperException, IOException {
		Random random = new Random();
		Configuration conf = getConf();

		int maxIndex = SCHEMA.getFields().size() - 1;

		Map<String, Class> customComparators = new HashMap<String, Class>();
		customComparators.put("thrift_field", AComparator.class);

		for(int randomSchema = 0; randomSchema < MAX_RANDOM_SCHEMAS; randomSchema++) {

			Schema schema = permuteSchema(SCHEMA);
			System.out.println("Schema : " + schema);

			for(int minIndex = maxIndex; minIndex >= 0; minIndex--) {
				Sorting sortCriteria = createRandomSortCriteria(schema, customComparators, maxIndex + 1);
				String[] groupFields = getFirstFields(sortCriteria.getSortCriteria(), random.nextInt(sortCriteria.getSortCriteria().getSortElements().length));

				/*
				 * Set everything into the Hadoop Conf.
				 */
				PangoolConfigBuilder builder = new PangoolConfigBuilder();
				builder.setGroupByFields(groupFields);
				builder.setSorting(sortCriteria);
				builder.addSchema(0, schema);
				PangoolConfig config = builder.build();
				PangoolConfig.setPangoolConfig(config, conf);
				
				SortComparator sortComparator = new SortComparator();
				GroupComparator groupComparator = new GroupComparator();

				sortComparator.setConf(conf);
				groupComparator.setConf(conf);

				for(int i = 0; i < MAX_RANDOMS_PER_INDEX; i++) {

					ITuple base1 = new BaseTuple();
					ITuple base2 = new BaseTuple();
					ITuple doubleBuffered1 = new Tuple(); // double buffered
					ITuple doubleBuffered2 = new Tuple(); // double buffered

					ITuple[] tuples = new ITuple[] { base1, base2, doubleBuffered1, doubleBuffered2 };
					for(ITuple tuple : tuples) {
						fillWithRandom(SCHEMA, tuple, minIndex, maxIndex);
					}
					for(int indexTuple1 = 0; indexTuple1 < tuples.length; indexTuple1++) {
						for(int indexTuple2 = indexTuple1; indexTuple2 < tuples.length; indexTuple2++) {
							ITuple tuple1 = tuples[indexTuple1];
							ITuple tuple2 = tuples[indexTuple2];
							assertSameComparison("Sort comparator", sortComparator, tuple1, tuple2);
							assertOppositeOrEqualsComparison(sortComparator, tuple1, tuple2);
							assertSameComparison("Group comparator", groupComparator, tuple1, tuple2);
							assertOppositeOrEqualsComparison(groupComparator, tuple1, tuple2);
						}
					}
				} // do you like bracket dance ?
			}
		}
	}

	private int compareInBinary1(SortComparator comp, ITuple tuple1, ITuple tuple2) throws IOException {
		TupleSerialization serialization = new TupleSerialization();
		Serializer ser = serialization.getSerializer(tuple1.getClass());
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.open(buffer1);
		ser.serialize(tuple1);
		ser.close();

		
		ser = serialization.getSerializer(tuple2.getClass());
		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.open(buffer2);
		ser.serialize(tuple2);
		ser.close();

		return comp.compare(buffer1.getData(), 0, buffer1.getLength(), buffer2.getData(), 0, buffer2.getLength());
	}
	
	
	private int compareInBinary2(SortComparator comp, ITuple tuple1, ITuple tuple2) throws IOException {
		Serialization ser = getSer();
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(tuple1, buffer1);
		ser.ser(tuple2,buffer2);
		return comp.compare(buffer1.getData(), 0, buffer1.getLength(), buffer2.getData(), 0, buffer2.getLength());
	}
	

	private String concatFields(String[] fields) {
		StringBuilder b = new StringBuilder();
		b.append(fields[0]);
		for(int i = 1; i < fields.length; i++) {
			b.append(",").append(fields[i]);
		}
		return b.toString();
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
			String[] groupFields = GroupComparator.getGroupComparatorFields(comparator.getConf());

			String error = alias + ",Not same comparison : Comp objects:'" + compObjects + "' Comp binary:'" + compBinary
			    + "' for tuples:" + "\nTUPLE1:" + tuple1 + "\nTUPLE2:" + tuple2 + "\nCRITERIA:"
			    + comparator.getConfig().getSorting().getSortCriteria() + "\nGROUP_FIELDS:" + concatFields(groupFields);

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
			    + "\nTUPLE1:" + tuple1 + "\nTUPLE2:" + tuple2 + "\nCRITERIA:"
			    + comp.getConfig().getSorting().getSortCriteria());
		}

		comp1 = compareInBinary1(comp, tuple1, tuple2);
		comp2 = compareInBinary1(comp, tuple2, tuple1);
		if(comp1 > 0 && comp2 > 0 || comp1 < 0 && comp2 < 0) {
			Assert.fail("Same comparison in BINARY: " + comp1 + " , " + comp2 + ".It should be opposite" + "' for tuples:"
			    + "\nTUPLE1:" + tuple1 + "\nTUPLE2:" + tuple2 + "\nCRITERIA:"
			    + comp.getConfig().getSorting().getSortCriteria());
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
		private Serialization ser;

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

	/**
	 * Fills the fields specified by the range (minIndex,maxIndex) with random data.
	 * 
	 */
  private void fillWithRandom(Schema schema, ITuple tuple, int minIndex, int maxIndex) {
		try {
			Random random = new Random();
			for(int i = minIndex; i <= maxIndex; i++) {
				Field field = schema.getField(i);
				String fieldName = field.getName();
				Class fieldType = field.getType();
				if(fieldType == Integer.class || fieldType == VIntWritable.class) {
					tuple.setInt(fieldName, random.nextInt());
				} else if(fieldType == Long.class || fieldType == VLongWritable.class) {
					tuple.setLong(fieldName, random.nextLong());
				} else if(fieldType == Boolean.class) {
					tuple.setBoolean(fieldName, random.nextBoolean());
				} else if(fieldType == Double.class) {
					tuple.setDouble(fieldName, random.nextDouble());
				} else if(fieldType == Float.class) {
					tuple.setFloat(fieldName, random.nextFloat());
				} else if(fieldType == String.class) {
					if(random.nextBoolean()) {
						tuple.setString(fieldName, "");
					} else {
						tuple.setString(fieldName, random.nextLong() + "");
					}
				} else if(fieldType.isEnum()) {
					Method method = fieldType.getMethod("values", new Class[0]);
					Enum[] values = (Enum[]) method.invoke(null);
					tuple.setEnum(fieldName, values[random.nextInt(values.length)]);
				} else {
					boolean toInstance = random.nextBoolean();
					if(toInstance) {
						Object instance = ReflectionUtils.newInstance(fieldType, null);
						tuple.setObject(fieldName, instance);
					} else {
						tuple.setObject(fieldName, null);
					}
				}
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Creates a copy of the schema with the fields shuffled.
	 */
	private static Schema permuteSchema(Schema schema) {
		List<Field> fields = schema.getFields();
		List<Field> permutedFields = new ArrayList<Field>(fields);
		Collections.shuffle(permutedFields);
		SchemaBuilder builder = new SchemaBuilder();
		for(Field field : permutedFields) {
			try {
				builder.add(field.getName(), field.getType());
			} catch(InvalidFieldException e) {
				throw new RuntimeException(e);
			}
		}
		return builder.createSchema();
	}

	/**
	 * 
	 * Creates a random sort criteria based in the specified schema.
	 */
	private static Sorting createRandomSortCriteria(Schema schema, Map<String, Class> customComparators,
	    int numFields) {
		try {
			Random random = new Random();
			SortingBuilder builder = new SortingBuilder();
			for(int i = 0; i < numFields; i++) {
				Field field = schema.getField(i);

				builder.add(field.getName(), random.nextBoolean() ? SortOrder.ASC : SortOrder.DESC,
				    customComparators.get(field.getName()));
			}
			return builder.buildSorting();
		} catch(InvalidFieldException e) {
			throw new RuntimeException(e);
		}
	}

	private String[] getFirstFields(SortCriteria sortCriteria, int numFields) {
		String[] result = new String[numFields];
		for(int i = 0; i < numFields; i++) {
			SortElement element = sortCriteria.getSortElements()[i];
			result[i] = element.getFieldName();
		}
		return result;
	}
}