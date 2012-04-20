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
package com.datasalt.pangool.io;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.Schema.Field;

/**
 * This is the basic implementation of {@link ITuple}.
 */
@SuppressWarnings("serial")
public class Tuple implements ITuple, Serializable {

	private Object[] array;
	private Schema schema;

	public Tuple(Schema schema) {
		this.schema = schema;
		int size = schema.getFields().size();
		this.array = new Object[size];
	}

	@Override
	public Object get(int pos) {
		return array[pos];
	}

	@Override
	public void set(int pos, Object object) {
		array[pos] = object;
	}

	@Override
	public String toString() {
		return toString(this);
	}

	public static String toString(ITuple tuple) {
		Schema schema = tuple.getSchema();
		StringBuilder b = new StringBuilder();
		b.append("{");
		for(int i = 0; i < schema.getFields().size(); i++) {
			Field f = schema.getField(i);
			if(i != 0) {
				b.append(",");
			}
			b.append("\"").append(f.getName()).append("\"").append(":");
			switch(f.getType()) {
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case BOOLEAN:
				b.append(tuple.get(i));
				break;
			case STRING:
			case ENUM:
				
				b.append("\"").append(tuple.get(i)).append("\"");
				break;
			case OBJECT:
				b.append("{").append(tuple.get(i).toString()).append("}");
				break;
			case BYTES:
				Object o = tuple.get(i);
				b.append("{\"bytes\": \"");
				byte[] bytes;
				int offset,length;
				if (o instanceof ByteBuffer) {
					ByteBuffer byteBuffer =(ByteBuffer)o;
		      bytes = byteBuffer.array();
		      offset = byteBuffer.arrayOffset() + byteBuffer.position();
		      length=byteBuffer.limit() -byteBuffer.position();
				}	else {
					//byte[]
					bytes = (byte[])o;
					offset = 0;
					length = bytes.length;
				}
		    for (int p = offset; p < offset+length; p++){
		       b.append((char)bytes[p]);
		    }
		    b.append("\"}");
			break;
			default:
				throw new PangoolRuntimeException("Not stringifiable type :" + f.getType());
			}
		}
		b.append("}");
		return b.toString();
	}

	@Override
	public void clear() {
		for(int i = 0; i < array.length; i++) {
			array[i] = null;
		}
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public Object get(String field) {
		Integer pos = schema.getFieldPos(field);
		if (pos == null){
			throw new IllegalArgumentException("Field '" + field +"' not present in schema "+schema);
		}
		return get(pos);
	}

	@Override
	public void set(String field, Object object) {
		Integer pos = schema.getFieldPos(field);
		if (pos == null){
			throw new IllegalArgumentException("Field '" + field +"' not present in schema "+schema);
		}
		set(pos, object);
	}

	public boolean equals(Object that) {
		if(!(that instanceof ITuple)) {
			return false;
		}
		boolean schemaEquals = this.schema.equals(((ITuple) that).getSchema());
		if(!schemaEquals) {
			return false;
		}

		for(int i = 0; i < array.length; i++) {
			Object o1 = get(i);
			Object o2 = ((ITuple) that).get(i);
			if(o1 == null) {
				if(o2 != null) {
					return false;
				}
			} else {
				// TODO this special case shouldn't be treated here.Tuples don't care
				// about Texts or Strings.
				// Create a new equals method outside that takes in consideration
				// this particular case concerning Serialization/deser
				if(o1 instanceof Text) {
					o1 = o1.toString();
				}
				if(o2 != null && o2 instanceof Text) {
					o2 = o2.toString();
				}
				if(!o1.equals(o2)) {
					return false;
				}
			}
		}
		return true;
	}

	public int hashCode() {
	  assert false : "hashCode not designed";
	  return 42; // any arbitrary constant will do 
	}
}
