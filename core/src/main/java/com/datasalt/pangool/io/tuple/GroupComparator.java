package com.datasalt.pangool.io.tuple;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * @author pere
 *
 */
public class GroupComparator extends SortComparator {

	private int numFieldsCompared;
	private static final String CONF_GROUP_COMPARATOR_FIELDS = GroupComparator.class.getName() + ".group.comparator.fields";

	@Override
	public int compare(SourcedTuple w1, SourcedTuple w2) {
		return compare(numFieldsCompared, commonSchema, commonCriteria, w1, w2);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return compare(numFieldsCompared, commonSchema, commonCriteria, b1, s1, l1, b2, s2, l2);
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		String[] fieldsToCompare = getGroupComparatorFields(conf);
		numFieldsCompared = (fieldsToCompare == null) ? 0 : fieldsToCompare.length;
	}

	public static void setGroupComparatorFields(Configuration conf, List<String> fields) {
		conf.setStrings(CONF_GROUP_COMPARATOR_FIELDS, fields.toArray(new String[0]));
	}

	public static String[] getGroupComparatorFields(Configuration conf) {
		return conf.getStrings(CONF_GROUP_COMPARATOR_FIELDS);
	}
}
