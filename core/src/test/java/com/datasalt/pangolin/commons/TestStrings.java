package com.datasalt.pangolin.commons;

import static com.datasalt.pangolin.commons.Strings.join;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

public class TestStrings {

	private static String[] empty = new String[]{};
	private static String[] a = new String[]{"a"};
	private static String[] ab = new String[]{"a", "b"};
	private static String[] abc = new String[]{"a", "b", "c"};
	
	@Test
	public void testJoinStringArrayString() {
		assertEquals("", join((String[]) null, ","));
		assertEquals("", join(empty, ","));
		assertEquals("a", join(a, ","));
		assertEquals("a,b", join(ab, ","));
		assertEquals("a,,b,,c", join(abc, ",,"));
	}

	@Test
	public void testJoinCollectionOfStringString() {
		assertEquals("", join((Collection<String>) null, ","));
		assertEquals("", join(asList(empty), ","));
		assertEquals("a", join(asList(a), ","));
		assertEquals("a,b", join(asList(ab), ","));
		assertEquals("a,,b,,c", join(asList(abc), ",,"));
	}

}
