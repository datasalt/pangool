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

import com.datasalt.pangool.io.Schema.Field.FieldSerializer;
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
public class SerializerComparator implements RawComparator<Object>, Serializable, Configurable {

	protected transient Configuration conf;
	protected transient HadoopSerialization hadoopSer;
	
	private transient DataOutputBuffer buf1;
	private transient DataOutputBuffer buf2;
	
	
	public int compare(Object o1,FieldSerializer ser1,Object o2,FieldSerializer ser2){
		try {
			if (o1 == null) {
				return (o2 == null) ? 0 : -1; 
			} else if (o2 == null) {
				return 1;
			}

			buf1.reset();
			if (ser1 == null){
				hadoopSer.ser(o1,buf1);
			} else {
				ser1.open(buf1);
				ser1.serialize(o1);
				ser1.close();
			}
			buf2.reset();
			if (ser2 == null){
				hadoopSer.ser(o2,buf2);
			} else {
				ser2.open(buf2);
				ser2.serialize(o2);
				ser2.close();
			}
		
			return compare(buf1.getData(), 0, buf1.getLength(), buf2.getData(), 0, buf2.getLength());
    } catch(IOException e) {
    	throw new RuntimeException(e);
    }
	}
	
	@Override
  public int compare(Object o1, Object o2) {
		return compare(o1,null,o2,null);
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
	    hadoopSer = new HadoopSerialization(conf);
    } catch(IOException e) {
    	throw new RuntimeException(e);
    }
  }

	@Override
  public Configuration getConf() {
		return conf;
  }
}
