package com.datasalt.pangool.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Criteria;
import com.datasalt.pangool.Criteria.SortElement;
import com.datasalt.pangool.io.tuple.ITuple;

public class GroupComparator extends SortComparator {

	private Criteria groupSortBy;
	@Override
	public int compare(ITuple w1, ITuple w2) {
		//TODO
		return 0;

	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try{
		Schema commonSchema = serInfo.getCommonSchema();
		return compare(b1,s1,b2,s2,commonSchema,groupSortBy,offsets);
		} catch(IOException e){
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void setConf(Configuration conf){
		super.setConf(conf);
		if (conf != null){
			List<SortElement> sortElements = grouperConf.getCommonSortBy().getElements();
			int numGroupByFields = grouperConf.getGroupByFields().size();
			List<SortElement> groupSortElements = new ArrayList<SortElement>();
			groupSortElements.addAll(sortElements);
			groupSortElements = groupSortElements.subList(0,numGroupByFields);
			groupSortBy = new Criteria(groupSortElements);
		}
	}

	
}
