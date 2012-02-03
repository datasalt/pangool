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
package com.datasalt.pangool.io.tuple;

import java.io.Serializable;

import org.apache.avro.util.Utf8;

import com.datasalt.pangool.Schema;

/**
 * This is the basic implementation of {@link ITuple}. It extends a HashMap<String, Object>
 */
@SuppressWarnings("serial")
public class Tuple extends BaseTuple implements Serializable {

	private Object[] array;
	private Schema schema;

	public Tuple(Schema schema) {
		this.schema = schema;
		int size = schema.getFields().length;
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

	
	/**
	 * Calculates a combinated hashCode using the specified number of fieldsfields.
	 * 
	 */
	public static int partialHashCode(ITuple tuple,int nFields) {
		int result = 0;
		for(int i = 0; i < nFields; i++) {
			result = result * 31 + tuple.get(i).hashCode();
		}
		return result & Integer.MAX_VALUE;
	}

	@Override
	public String toString() {
		String str = "";
		for(Object obj: array) {
			if(obj instanceof byte[]) {
				str += new Utf8((byte[])obj).toString() + ",";
			} else {
				str += obj.toString() + ",";
			}
		}
		return str.substring(0, str.length() - 1);
	}

	@Override
  public void clear() {
		if (array!= null){
			for (int i=0 ; i < array.length ; i++){
				array[i] = null;
			}
		}
  }

	@Override
  public Schema getSchema() {
	  return schema;
  }

		@Override
  public Object get(String field) {
	  return get(schema.getFieldPos(field));
  }

	@Override
  public void set(String field, Object object) {
	  set(schema.getFieldPos(field),object);
  }
}
