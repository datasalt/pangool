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
package com.datasalt.pangool.benchmark;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

/**
 * RawComparator that compares using an Avro schema. This Comparator can be used as Group Comparator for binary
 * secondary sorting. 
 * <p>
 * Property this.{@link #GROUP_SCHEMA} can be used in Hadoop Configuration for serializing the Schema to use.
 * <p>
 * If multiple Group Comparators are needed in a single Configuration instance, this class needs to be subclassed
 * in order to read from different Hadoop Configuration properties.
 */
@SuppressWarnings("rawtypes")
public class AvroGroupComparator implements RawComparator, Configurable {

	public final static String GROUP_SCHEMA = AvroGroupComparator.class.getName() + ".group.schema";
	Schema schema;
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return BinaryData.compare(b1, s1, l1, b2, s2, l2, schema); 
	}

	@Override
	public int compare(Object obj1, Object obj2) {
    throw new NotImplementedException();
	}

	@Override
  public Configuration getConf() {
    throw new NotImplementedException();
  }

	@SuppressWarnings("deprecation")
  @Override
  public void setConf(Configuration conf) {
		schema = Schema.parse(conf.get(GROUP_SCHEMA));
	}
};
