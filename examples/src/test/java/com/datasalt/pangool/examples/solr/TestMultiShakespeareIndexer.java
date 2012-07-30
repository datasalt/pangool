package com.datasalt.pangool.examples.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestMultiShakespeareIndexer extends AbstractHadoopTestLibrary {

	public final static String INPUT = "src/test/resources/shakespeare-input";
	public final static String OUTPUT = "out-" + TestMultiShakespeareIndexer.class.getName();

	@Test
	public void test() throws Exception {
		trash(OUTPUT);
		
		ToolRunner.run(getConf(),new MultiShakespeareIndexer(), new String[] { INPUT, OUTPUT });
		
		// Assert that indexes have been created
		for(String category: new String[] { "comedies", "histories", "tragedies", "poetry" }) {
			assertTrue(new File(OUTPUT + "/" + category + "/part-00000/data/index").exists());
			assertTrue(new File(OUTPUT + "/" + category + "/part-00000/conf/schema.xml").exists());
			assertTrue(new File(OUTPUT + "/" + category + "/part-00000/conf/solrconfig.xml").exists());
			assertTrue(new File(OUTPUT + "/" + category + "/part-00000/conf/stopwords.txt").exists());
		}		
		
		// Validate data inside index
		IndexReader r = IndexReader.open(FSDirectory.open(new File(OUTPUT + "/comedies/part-00000/data/index")));
		assertEquals(1, r.maxDoc());
		String document = r.document(0).toString();
		assertTrue(document.contains("comedy"));
		
		r = IndexReader.open(FSDirectory.open(new File(OUTPUT + "/histories/part-00000/data/index")));
		assertEquals(1, r.maxDoc());
		document = r.document(0).toString();
		assertTrue(document.contains("history"));
		
		r = IndexReader.open(FSDirectory.open(new File(OUTPUT + "/tragedies/part-00000/data/index")));
		assertEquals(1, r.maxDoc());
		document = r.document(0).toString();
		assertTrue(document.contains("tragedy"));
		
		r = IndexReader.open(FSDirectory.open(new File(OUTPUT + "/poetry/part-00000/data/index")));
		assertEquals(1, r.maxDoc());
		document = r.document(0).toString();
		assertTrue(document.contains("poetry"));

		trash(OUTPUT);
	}
}
