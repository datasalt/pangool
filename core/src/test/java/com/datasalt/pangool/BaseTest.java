package com.datasalt.pangool;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Random;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;

import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortCriteria.SortOrder;
import com.datasalt.pangool.io.Serialization;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.test.AbstractBaseTest;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class BaseTest extends AbstractBaseTest {

	public static Schema SCHEMA;

	@Before
	public void prepareBaseSchema() throws CoGrouperException, IOException, InvalidFieldException {
		SCHEMA = Schema.parse("int_field:int," + "long_field:long," + "vint_field:vint," + "vlong_field:vlong,"
		    + "float_field:float," + "double_field:double," + "string_field:string," + "boolean_field:boolean,"
		    + "enum_field:" + SortOrder.class.getName() + "," + "thrift_field:" + A.class.getName());
	}

	/**
	 * Fills the fields specified by the range (minIndex, maxIndex) with random data.
	 * 
	 */
	protected static void fillTuple(boolean isRandom, Schema schema, ITuple tuple, int minIndex, int maxIndex) {
		try {
			Random random = new Random();
			for(int i = minIndex; i <= maxIndex; i++) {
				Field field = schema.getField(i);
				Class fieldType = field.getType();
				if(fieldType == Integer.class || fieldType == VIntWritable.class) {
					tuple.setInt(i, isRandom ? random.nextInt() : 0);
				} else if(fieldType == Long.class || fieldType == VLongWritable.class) {
					tuple.setLong(i, isRandom ? random.nextLong() : 0);
				} else if(fieldType == Boolean.class) {
					tuple.setBoolean(i, isRandom ? random.nextBoolean() : false);
				} else if(fieldType == Double.class) {
					tuple.setDouble(i, isRandom ? random.nextDouble() : 0.0);
				} else if(fieldType == Float.class) {
					tuple.setFloat(i, isRandom ? random.nextFloat() : 0f);
				} else if(fieldType == String.class) {
					if(!isRandom || random.nextBoolean()) {
						tuple.setString(i, Utf8.getBytesFor(""));
					} else {
						tuple.setString(i, Utf8.getBytesFor(random.nextLong() + ""));
					}
				} else if(fieldType.isEnum()) {
          Method method = fieldType.getMethod("values", (Class[])null);
					Enum[] values = (Enum[]) method.invoke(null);
					tuple.setEnum(i, values[isRandom ? random.nextInt(values.length) : 0]);
				} else {
					boolean toInstance = random.nextBoolean();
					if(isRandom && toInstance) {
						Object instance = ReflectionUtils.newInstance(fieldType, null);
						tuple.setObject(i, instance);
					} else {
						tuple.setObject(i, null);
					}
				}
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected void assertSerializable(Serialization ser,ITuple tuple, boolean debug) throws IOException {

		DataInputBuffer input = new DataInputBuffer();
		DataOutputBuffer output = new DataOutputBuffer();

		ser.ser(tuple, output);

		input.reset(output.getData(), 0, output.getLength());
		ITuple deserializedTuple = new DoubleBufferedTuple();
		deserializedTuple = ser.deser(deserializedTuple, input);
		if(debug) {
			System.out.println("D:" + deserializedTuple);
		}
		assertEquals(tuple, deserializedTuple);
	}

}
