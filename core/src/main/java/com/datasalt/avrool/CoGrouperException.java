package com.datasalt.avrool;

/**
 * 
 * 
 *
 */
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
}