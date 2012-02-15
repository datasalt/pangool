package com.datasalt.pangool.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Criteria;
import com.datasalt.pangool.Criteria.SortElement;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.io.tuple.ITuple;

public class GroupComparator extends SortComparator {

	private Criteria groupSortBy;
	
	public GroupComparator(){}
	public GroupComparator(CoGrouperConfig grouperConfig){
		
	}
	
	
	@Override
	public int compare(ITuple w1, ITuple w2) {
		int sourceId1 = grouperConf.getSourceIdByName(w1.getSchema().getName());
		int sourceId2 = grouperConf.getSourceIdByName(w2.getSchema().getName());
		int[] indexes1 = serInfo.getCommonSchemaIndexTranslation(sourceId1);
		int[] indexes2 = serInfo.getCommonSchemaIndexTranslation(sourceId2);
		return compare(groupSortBy, w1, indexes1, w2, indexes2);
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
		try{
		if (conf != null){
			setGrouperConf(CoGrouperConfig.get(conf));
		}
		} catch(CoGrouperException e){
			throw new RuntimeException(e);
		}
	}
	
	@Override
	protected void setGrouperConf(CoGrouperConfig config){
		super.setGrouperConf(config);
		List<SortElement> sortElements = grouperConf.getCommonCriteria().getElements();
		int numGroupByFields = grouperConf.getGroupByFields().size();
		List<SortElement> groupSortElements = new ArrayList<SortElement>();
		groupSortElements.addAll(sortElements);
		groupSortElements = groupSortElements.subList(0,numGroupByFields);
		groupSortBy = new Criteria(groupSortElements);
		
	}
	
}
