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

import com.datasalt.pangool.mapreduce.SortComparator;

/**
 * This is the basic implementation of {@link ITuple}. It extends a HashMap<String, Object>
 */
@SuppressWarnings("serial")
public class Tuple implements ITuple, Serializable {

	Object[] array;

	public Tuple() {
	}

	public Tuple(int size) {
		this.array = new Object[size];
	}
	
	public Tuple(Object[] array) {
		this.array = array;
	}

	public Object[] getArray() {
		return array;
	}

	@Override
	public Integer getInt(int pos) {
		return (Integer) array[pos];
	}

	@Override
	public Long getLong(int pos) {
		return (Long) array[pos];
	}

	@Override
	public Float getFloat(int pos) {
		return (Float) array[pos];
	}

	@Override
	public Double getDouble(int pos) {
		return (Double) array[pos];
	}

	@Override
	public byte[] getString(int pos) {
		return (byte[]) array[pos];
	}

	@Override
	public Object getObject(int pos) {
		return array[pos];
	}

	@Override
	public Enum<?> getEnum(int pos) {
		return (Enum<?>) array[pos];
	}

	private void setField(int pos, Object obj) {
		array[pos] = obj;
	}

	@Override
	public void setEnum(int pos, Enum<? extends Enum<?>> value) {
		setField(pos, value);
	}

	@Override
	public void setInt(int pos, int value) {
		setField(pos, value);
	}

	@Override
	public void setString(int pos, byte[] value) {
		setField(pos, value);
	}

	@Override
	public void setLong(int pos, long value) {
		setField(pos, value);
	}

	@Override
	public void setFloat(int pos, float value) {
		setField(pos, value);
	}

	@Override
	public void setDouble(int pos, double value) {
		setField(pos, value);
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		setField(pos, value);
	}

	@Override
	public void setObject(int pos, Object object) {
		setField(pos, object);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject(Class<T> clazz, int pos) {
		return (T) array[pos];
	}

	@Override
	public <T> void setObject(Class<T> valueType, int pos, T value) {
		setField(pos, value);
	}

	/**
	 * Calculates a combinated hashCode using the specified number of fieldsfields.
	 * 
	 * @param fields
	 * 
	 */
	@Override
	public int partialHashCode(int nFields) {
		int result = 0;
		for(int i = 0; i < nFields; i++) {
			result = result * 31 + array[i].hashCode();
		}
		return result & Integer.MAX_VALUE;
	}

	@Override
	public int compareTo(ITuple that) {
		if(array.length != that.getArray().length) {
			return array.length > that.getArray().length ? -1 : 1;
		}
		for(int i = 0; i < array.length; i++) {
			int comparison = SortComparator.compareObjects(array[i], that.getArray()[i]);
			if(comparison != 0) {
				return comparison;
			}
		}
		return 0;
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
	public int size() {
		return array.length;
	}

	@Override
	public void clear() {
		// Nothing to be done
	}

	@Override
	public void setArray(Object[] array) {
		this.array = array;
	}
}
