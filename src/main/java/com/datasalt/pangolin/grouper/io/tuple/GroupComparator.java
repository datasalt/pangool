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

package com.datasalt.pangolin.grouper.io.tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author eric
 *
 */
public class GroupComparator extends SortComparator{

	private int numFieldsCompared;
	private static final String CONF_GROUP_COMPARATOR_FIELDS = "datasalt.pangolin.grouper.group_comparator.fields";
	
	@Override
	public int compare(Object w1,Object w2) {
		return compare((WritableComparable)w1,(WritableComparable)w2);
	}
	
	@Override
	public int compare(WritableComparable w1,WritableComparable w2) {
		return compare(numFieldsCompared,w1,w2);
	}
	
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return compare(numFieldsCompared,b1,s1,l1,b2,s2,l2);
	}

	@Override
  public void setConf(Configuration conf) {
	  super.setConf(conf);
	  String[] fieldsToCompare = getGroupComparatorFields(conf);
	  numFieldsCompared = (fieldsToCompare == null) ? 0 : fieldsToCompare.length;
  }
	
	public static void setGroupComparatorFields(Configuration conf,String[] fields){
		conf.setStrings(CONF_GROUP_COMPARATOR_FIELDS, fields);
	}
	
	public static String[] getGroupComparatorFields(Configuration conf){
		return conf.getStrings(CONF_GROUP_COMPARATOR_FIELDS);
	}
	
	
}
