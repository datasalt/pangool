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
package com.datasalt.pangool.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestSolrOutputFormat extends AbstractHadoopTestLibrary {

	public final static String INPUT = "src/test/resources/user-messages.txt";
	public final static String OUTPUT = "out-" + TestSolrOutputFormat.class.getName();

	@Test
	public void test() throws Exception {
		trash(OUTPUT);

		TupleSolrOutputFormatExample example = new TupleSolrOutputFormatExample();
		example.run(INPUT, OUTPUT, getConf());

		// Assert that indexes have been created
		assertTrue(new File(OUTPUT + "/part-00000/data/index").exists());
		assertTrue(new File(OUTPUT + "/FR/part-00000/data/index").exists());
		assertTrue(new File(OUTPUT + "/ES/part-00000/data/index").exists());

		// Validate data inside index
		IndexReader r = IndexReader.open(FSDirectory.open(new File(OUTPUT + "/part-00000/data/index")));
		assertEquals(2, r.maxDoc());

		int contentAssertions = 0;
		Set<String> distinctMessages = new HashSet<String>();

		for(int i = 0; i < 2; i++) {
			String document = r.document(i).toString();
			distinctMessages.add(document);
			if(document.contains("user_id:user1")) {
				assertTrue(document.contains("Hi, this is a message from user1."));
				contentAssertions++;
			} else if(document.contains("user_id:user4")) {
				assertTrue(document.contains("Hi, this is another message from user4."));
				contentAssertions++;
			}
		}

		assertEquals(2, distinctMessages.size());
		assertEquals(2, contentAssertions);

		r = IndexReader.open(FSDirectory.open(new File(OUTPUT + "/FR/part-00000/data/index")));
		assertEquals(1, r.maxDoc());

		String document = r.document(0).toString();
		assertTrue(document.contains("user_id:user3"));
		assertTrue(document.contains("Oh la lá!"));

		r = IndexReader.open(FSDirectory.open(new File(OUTPUT + "/ES/part-00000/data/index")));
		assertEquals(1, r.maxDoc());
		
		document = r.document(0).toString();
		assertTrue(document.contains("user_id:user2"));
		assertTrue(document.contains("Yo no hablo inglés."));

		document = r.document(0).toString();

		trash(OUTPUT);
	}
}
