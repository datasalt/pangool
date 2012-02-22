package com.datasalt.pangool.utils;

import java.util.Collection;

public class Strings {

	public static String join(String []strings, String connector) {
		if (strings == null) {
			return "";
		}
		
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<strings.length; i++) {
			if (i!=0) {
				sb.append(connector);
			}
			sb.append(strings[i]);
		}
		
		return sb.toString();
	}
	
	public static String join(Collection<String> strings, String connector) {
		if (strings == null) {
			return "";
		}

		StringBuilder sb = new StringBuilder();
		
		boolean first = true;
		for (String s: strings) {
			if (first) {
				first = false;
			} else {
				sb.append(connector);
			}
			sb.append(s);
		}
		return sb.toString();		
	}
	
}
