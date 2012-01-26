package com.datasalt.avrool.mapreduce;

import org.apache.hadoop.conf.Configuration;

import com.datasalt.avrool.io.tuple.ITuple;

public class GroupComparator extends SortComparator {

	private int numFieldsCompared;

	@Override
	public int compare(ITuple w1, ITuple w2) {
		return compare(numFieldsCompared, commonSchema, commonCriteria, w1, w2);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return compare(numFieldsCompared, commonSchema, commonCriteria, b1, s1, l1, b2, s2, l2);
	}

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		numFieldsCompared = (config.getGroupByFields() == null) ? 0 : config.getGroupByFields().size();
	}
}
