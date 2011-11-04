package com.datasalt.pangolin.commons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

public class CommonUtils {

	@SuppressWarnings({ "rawtypes", "unchecked" })
  public static Map invertMap(Map<?,?> map){
		Map result = new TreeMap();
		for (Map.Entry entry : map.entrySet()){
  		result.put(entry.getValue(),entry.getKey());
  	}
		return result;
	}
	
	
	
	
}
