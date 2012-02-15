package com.datasalt.pangool;

import java.util.Collection;


public class CoGrouperException extends Exception {

	private static final long serialVersionUID = 1L;

	public CoGrouperException(Throwable e) {
		super(e);
	}

	public CoGrouperException(String message, Throwable e) {
		super(message, e);
	}

	public CoGrouperException(String message) {
		super(message);
	}
	
	public static void failIfNull(Object ob, String message) throws CoGrouperException {
		if(ob == null) {
			throw new CoGrouperException(message);
		}
	}

	public static void failIfEmpty(Collection ob, String message) throws CoGrouperException {
		if(ob == null || ob.isEmpty()) {
			throw new CoGrouperException(message);
		}
	}
	
	public static void failIfEmpty(Object[] ob, String message) throws CoGrouperException {
		if(ob == null || ob.length == 0) {
			throw new CoGrouperException(message);
		}
	}

	public static void failIfNotNull(Object ob, String message) throws CoGrouperException {
		if(ob != null) {
			throw new CoGrouperException(message);
		}
	}
	
	
}