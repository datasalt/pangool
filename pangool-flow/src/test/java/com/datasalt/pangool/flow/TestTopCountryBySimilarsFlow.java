package com.datasalt.pangool.flow;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datasalt.pangool.flow.LinearFlow.EXECUTION_MODE;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

public class TestTopCountryBySimilarsFlow extends AbstractHadoopTestLibrary {

	public final static String OUT = "out-" + TestTopCountryBySimilarsFlow.class.getName();
	
	@Test
	public void test() throws Exception {
		String similarityTestFile  = "src/test/resources/test-similarity-input.txt";
		String countryInfoTestFile = "src/test/resources/test-country-info-input.txt";
		int topSize = 2;
		
		TopCountryBySimilarsFlow flow = new TopCountryBySimilarsFlow(similarityTestFile, countryInfoTestFile, topSize, OUT);
		flow.execute(EXECUTION_MODE.OVERWRITE, new Configuration(), "topCountry.output");
		
		final Map<String, String> resultMap = new HashMap<String, String>();
		Utils.readTuples(OUT + "/part-r-00000", new Utils.TupleVisitor() {

			@Override
      public void onTuple(ITuple tuple) {
				resultMap.put(tuple.get("first").toString(), tuple.get("country").toString());
      }
		});
		
		assertEquals("ES", resultMap.get("Jon"));
		assertEquals("UK", resultMap.get("Pere"));
		
		trash(OUT, "topSimilarities.output", "attachCountry.output");
	}
}
