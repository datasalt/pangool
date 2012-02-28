package com.datasalt.pangool.examples;

public class TestUtils {

	public static char randomChar() {
		return (char) (int) (Math.random() * 26.0D + 97.0D);
	}

	public static String randomString(int size) {
		String str = "";
		for(int i = 0; i < size; ++i) {
			str = str + randomChar();
		}
		return str;
	}
}
