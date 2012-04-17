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
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

import com.datasalt.pangool.io.Schema.Field.FieldDeserializer;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.serialization.HadoopSerialization;

@SuppressWarnings("serial")
public abstract class DeserializerComparator<T> implements RawComparator<T>, Serializable, Configurable {
	private Configuration conf;
  private T object1 = null;
  private T object2 = null;
  private FieldDeserializer fieldDeser; //
  private HadoopSerialization hadoopSer;
  private  DataInputBuffer tmpInputBuffer;
  
  public DeserializerComparator(){
  	
  }
  public DeserializerComparator(FieldDeserializer fieldDeser){
		this.fieldDeser = fieldDeser;
	}
	
	@Override
	public void setConf(Configuration conf) {
		try {
			this.conf = conf;
			hadoopSer = new HadoopSerialization(conf);
			
    } catch(IOException e) {
    	throw new RuntimeException(e);
    }
	}
	
	@Override
  public Configuration getConf() {
		return conf;
  }

	/**
	 * Objects can be null. 
	 */
	@Override
  public abstract int compare(T object1, T object2);
	
	@SuppressWarnings("unchecked")
  @Override
	public final int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			if(fieldDeser == null) {
				object1 = hadoopSer.deser(object1, b1, s1, l1);
				object2 = hadoopSer.deser(object2, b2, s2, l2);
			} else {
				if (tmpInputBuffer == null){
					tmpInputBuffer = new DataInputBuffer();
				}
				tmpInputBuffer.reset(b1, s1, l1);
				fieldDeser.open(tmpInputBuffer);
				object1 = (T) fieldDeser.deserialize(object1);
				fieldDeser.close();
				tmpInputBuffer.reset(b2, s2, l2);
				fieldDeser.open(tmpInputBuffer);
				object2 = (T) fieldDeser.deserialize(object2);
				fieldDeser.close();
			}
			return compare(object1, object2);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}	
}
