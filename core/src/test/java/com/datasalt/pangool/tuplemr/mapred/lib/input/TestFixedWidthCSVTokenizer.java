/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.tuplemr.mapred.lib.input;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;


public class TestFixedWidthCSVTokenizer extends TestCase {

	@Test
	public void testTokenizeLine() throws IOException {
		String line = "012345678901234567890";

		int fields[] = new int[]{0,0, 19,20, 5,10, 5,15};
		FixedWidthCSVTokenizer fwt = new FixedWidthCSVTokenizer(fields, null);
		
		List<String> tokens = fwt.tokenizeLine(line, null, null);
		
		String[] results = new String[] {"0", "90", "567890", "56789012345"};
		assertEquals(4, tokens.size());
		int c = 0;
		for (String token: tokens) {
			assertEquals(results[c++], token);			
		}
		
		// Null test
		fwt = new FixedWidthCSVTokenizer(fields, "90");
		tokens = fwt.tokenizeLine(line, null, null);
		assertNull(tokens.get(1));
		assertNotNull(tokens.get(0));
		
		fwt = new FixedWidthCSVTokenizer(fields, "0");
		tokens = fwt.tokenizeLine(line, null, null);
		assertNull(tokens.get(0));
		assertNotNull(tokens.get(1));

		
		// Failure tests
		boolean fail = true;
		fields = new int[]{1,0,2};				
		try {
			fwt = new FixedWidthCSVTokenizer(fields, null);
		} catch (IllegalArgumentException e) {
			fail = false;
		}
		assertFalse(fail);

		
		fail = true;
		fields = new int[]{1,0};				
		try {
			fwt = new FixedWidthCSVTokenizer(fields, null);
			fwt.tokenizeLine(line, null, null);
		} catch (IllegalArgumentException e) {
			fail = false;
		}
		assertFalse(fail);
		
		fail = true;
		fields = new int[]{20,21};		
		fwt = new FixedWidthCSVTokenizer(fields, null);
		try {
			fwt.tokenizeLine(line, null, null);
		} catch (IOException e) {
			fail = false;
		}
		assertFalse(fail);
		
		fail = true;
		fields = new int[]{-1,1};				
		try {
			fwt = new FixedWidthCSVTokenizer(fields, null);
			fwt.tokenizeLine(line, null, null);
		} catch (IllegalArgumentException e) {
			fail = false;
		}
		assertFalse(fail);

		fail = true;
		fields = new int[]{-1,1};				
		try {
			fwt = new FixedWidthCSVTokenizer(fields, null);
			fwt.tokenizeLine(line, null, null);
		} catch (IllegalArgumentException e) {
			fail = false;
		}
		assertFalse(fail);		
	}
	
	
	@Test
	public void testNull() throws IOException {
		String line = " -   ";
                  
		int fields[] = new int[]{0,3, 3,4};
		FixedWidthCSVTokenizer fwt = new FixedWidthCSVTokenizer(fields, "-");
		
		List<String> tokens = fwt.tokenizeLine(line, null, null);
		assertNull(tokens.get(0));
		assertEquals("  ", tokens.get(1));
		
		fwt = new FixedWidthCSVTokenizer(fields, " ");		
		tokens = fwt.tokenizeLine(line, null, null);
		assertNull(tokens.get(1));
		assertEquals(" -  ", tokens.get(0));		
	}	
}
