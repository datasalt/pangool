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
package com.datasalt.pangolin.grouper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.Schema.Field;

/**
 * TODO
 * 
 * @author epalace
 * 
 */
public class Tuple extends BinaryComparable implements WritableComparable<BinaryComparable>,Configurable {

	private DataOutputBuffer dob = new DataOutputBuffer();
	private DataInputBuffer dib = new DataInputBuffer();
	private Buffer buf = new Buffer();
	
	private Comparable[] objects;
	private Schema schema;

	public Tuple() {
		
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
		// TODO this should erase previous state ?
		if (this.objects.length != schema.getFields().length) {
			this.objects = new Comparable[schema.getFields().length];
		}
	}

	public void setField(String fieldName, Comparable value) {
		int index = this.schema.getIndexByFieldName(fieldName);
		//TODO check that index is valid
		objects[index] = value;
	}

	public void setField(int index, Comparable value) {
		this.objects[index] = value;
	}

	private void serializeFieldsToBuffer() throws IOException {
		dob.reset();
		for (int numField = 0; numField < schema.getFields().length; numField++) {
			Class fieldType = schema.getFields()[numField].getType();
			if (fieldType == Integer.class) {
				WritableUtils.writeVInt(dob, (Integer) objects[numField]);
			} else if (fieldType == Long.class) {
				WritableUtils.writeVLong(dob, (Long) objects[numField]);
			} else if (fieldType == Double.class) {
				dob.writeDouble((Double) objects[numField]);
			} else if (fieldType == Float.class) {
				dob.writeFloat((Float) objects[numField]);
			} else if (fieldType == String.class) {
				dob.writeUTF((String) objects[numField]);
			} else {
				throw new RuntimeException("Not implemented fieldType : " + fieldType); // TODO
																																								// output
																																								// correct
																																								// exception
			}
		}
	}

	private void deserializeBufferToFields() throws IOException {
		int i=0;
		for (Field field : schema.getFields()) {
			
			Class fieldType = field.getType();
			if (fieldType == VIntWritable.class) {
				setField(i, WritableUtils.readVInt(dib));
			} else if (fieldType == VLongWritable.class) {
				setField(i, WritableUtils.readVLong(dib));
			} else if (fieldType == Double.class) {
				setField(i, dib.readDouble());
			} else if (fieldType == Float.class) {
				setField(i, dib.readFloat());
			} else if (fieldType == String.class) {
				setField(i, dib.readUTF()); // TODO use Text ?
			} else if (fieldType == TBase.class) { // TODO improve this .
				// TODO ..

			} else {
				// TODO
				throw new RuntimeException("Not implemented fieldType :  " + fieldType);
			}
			i++;
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO this can be cached
		//serializeFieldsToBuffer(); //maybe it's already serialized
		int size = dob.getLength();
		WritableUtils.writeVInt(output, size);
		output.write(dob.getData(),0,size);
		
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		int size = WritableUtils.readVInt(input);
		buf.setSize(size);
		input.readFully(buf.getBytes(), 0,size);
		dib.reset(buf.getBytes(),size);
		
	}

	public void set(Tuple tuple) {
		this.schema = tuple.schema;
		//TODO what to do in case of not being serialize to objects
		if (objects == null || objects.length != schema.getFields().length) {
			objects = new Comparable[schema.getFields().length];
		}
		for (int i = 0; i < schema.getFields().length; i++) {
			objects[i] = tuple.objects[i];
		}
	}

	/**
	 * returns the level where they mismatch. Returns 0 if equals
	 * 
	 * @param tuple1
	 * @param tuple2
	 * @param levels
	 * @return
	 */
	public static int compareLevels(Tuple tuple1, Tuple tuple2, int levels) {
		for (int i = 0; i < levels; i++) {
			int comparison = tuple1.objects[i].compareTo((tuple2.objects[i]));
			if (comparison != 0) {
				return i;
			}
		}
		return 0;
	}

	public int partialHashCode(int[] fieldsIndexes) {
		int result = 0;
		for (int fieldIndex : fieldsIndexes) {
			result = result * 31 + objects[fieldIndex].hashCode();
		}

		return result & Integer.MAX_VALUE;
	}

	@Override
	public byte[] getBytes() {
		//TODO serialize first
		return buf.getBytes();
	}

	@Override
	public int getLength() {
		//TODO serialize first
		return buf.getLength();
	}

	@Override
  public Configuration getConf() {
	  // TODO Auto-generated method stub
	  return null;
  }

	@Override
  public void setConf(Configuration conf) {
	  System.out.println("Tuple llamando setConf" + conf);
	  
  }

	
}
