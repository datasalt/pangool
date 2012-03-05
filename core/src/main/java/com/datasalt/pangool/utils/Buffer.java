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
package com.datasalt.pangool.utils;

public class Buffer {
	private int size;
	private byte[] bytes;

	public Buffer(){
		this(0);
	}
	
	public Buffer(int initialCapacity){
		this.bytes = new byte[initialCapacity];
	}
	
	
	public byte[] getBytes(){
		return bytes;
	}
	
	/**
	 * Get the current size of the buffer.
	 */
	public int getLength() {
		return size;
	}

	/**
	 * Change the size of the buffer. The values in the old range are preserved
	 * and any new values are undefined. The capacity is changed if it is
	 * necessary.
	 * 
	 * @param size
	 *          The new number of bytes
	 */
	public void setSize(int size) {
		if (size > getCapacity()) {
			setCapacity(size * 3 / 2);
		}
		this.size = size;
	}

	/**
	 * Get the capacity, which is the maximum size that could handled without
	 * resizing the backing storage.
	 * 
	 * @return The number of bytes
	 */
	public int getCapacity() {
		return bytes.length;
	}

	/**
	 * Change the capacity of the backing storage. The data is preserved.
	 * 
	 * @param new_cap
	 *          The new capacity in bytes.
	 */
	public void setCapacity(int new_cap) {
		if (new_cap != getCapacity()) {
			byte[] new_data = new byte[new_cap];
			if (new_cap < size) {
				size = new_cap;
			}
			if (size != 0) {
				System.arraycopy(bytes, 0, new_data, 0, size);
			}
			bytes = new_data;
		}
	}

}
