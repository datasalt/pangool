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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import com.datasalt.pangool.serialization.HadoopSerialization;

/**
 * A simple {@link RawComparator} and {@link Serializable} that
 * compares in binary. It also implements Comparator<Object>{@link #compare(Object, Object)}
 * by serializing the objects using {@link HadoopSerialization}, 
 * and then comparing the bytes. That is useful for testing purposes.
 * <br>
 * This class needs to receive a configuration via {@link #setConf(Configuration)} before
 * being ready to use the {@link #compare(Object, Object)} method.  
 */
@SuppressWarnings("serial")
public class BinaryComparator implements RawComparator<Object>, Serializable, Configurable {

	protected Configuration conf;
	protected HadoopSerialization ser;
	
	private DataOutputBuffer buf1;
	private DataOutputBuffer buf2;
	
	@Override
  public int compare(Object o1, Object o2) {		
		try {
			
			if (o1 == null) {
				return (o2 == null) ? 0 : -1; 
			} else if (o2 == null) {
				return 1;
			}
			
			buf1.reset();
	    ser.ser(o1, buf1);
	    
			buf2.reset();
	    ser.ser(o2, buf2);
	    
	    return compare(buf1.getData(), 0, buf1.getLength(), buf2.getData(), 0, buf2.getLength());
    } catch(IOException e) {
    	throw new RuntimeException(e);
    }
  }

	@Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
  }

	@Override
  public void setConf(Configuration conf) {
		buf1 = new DataOutputBuffer();
		buf2 = new DataOutputBuffer();
		
		try {
	    ser = new HadoopSerialization(conf);
    } catch(IOException e) {
    	throw new RuntimeException(e);
    }
  }

	@Override
  public Configuration getConf() {
		return conf;
  }
}
