package com.datasalt.pangool.examples.naivebayes;

import java.io.File;
import java.nio.charset.Charset;

import junit.framework.Assert;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.examples.naivebayes.NaiveBayesGenerate.Category;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestNaiveBayes extends AbstractHadoopTestLibrary{

	public static String OUT_MODEL = "out-model-" + TestNaiveBayes.class.getName();
	public static String OUT_CLASSIFY = "out-classify-" + TestNaiveBayes.class.getName();
	
	@Test
	public void test() throws Exception {
		trash(OUT_MODEL, OUT_CLASSIFY);
		
		ToolRunner.run(new NaiveBayesGenerate(), new String[] { "src/test/resources/nb-examples.txt", OUT_MODEL });
		ToolRunner.run(new NaiveBayesClassifier(), new String[] { OUT_MODEL + "/p*", "src/test/resources/nb-test-examples.txt", OUT_CLASSIFY });
		
		int assertionsMade = 0;
		
		for(String line: Files.readLines(new File(OUT_CLASSIFY + "/part-m-00000"), Charset.forName("UTF-8"))) {
			if(line.startsWith("Fantastic hotel")) {
				Assert.assertTrue(line.endsWith(Category.POSITIVE + "")); 
				assertionsMade++;
			} else if(line.startsWith("Very disapointing")) {
				Assert.assertTrue(line.endsWith(Category.NEGATIVE + ""));
				assertionsMade++;
			}
		}
		
		Assert.assertEquals(2, assertionsMade);
		trash(OUT_MODEL, OUT_CLASSIFY);
	}
}
