package com.datasalt.pangool.examples.topnhashtags;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.junit.Test;

import com.datasalt.pangool.examples.topnhashtags.Beans.Entities;
import com.datasalt.pangool.examples.topnhashtags.Beans.HashTag;
import com.datasalt.pangool.examples.topnhashtags.Beans.SimpleTweet;
import com.datasalt.pangool.examples.topnhashtags.Beans.UserInfo;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestTopNHashTags extends AbstractHadoopTestLibrary {

	public final static String INPUT = TestTopNHashTags.class.getName() + "-input";
	public final static String OUTPUT = TestTopNHashTags.class.getName() + "-output";
	
	@Test
	public void test() throws Exception {
		trash(OUTPUT);
		BufferedWriter writer = new BufferedWriter(new FileWriter(INPUT));
		
		long today = System.currentTimeMillis();
		DateTime dateTime = new DateTime(today);
		String todayDate = dateTime.getYear() + "-" + dateTime.getMonthOfYear() + "-" + dateTime.getDayOfMonth();
		long anotherDay = today - 60 * 60 * 1000 * 24 *10;
		dateTime = new DateTime(anotherDay);
		String anotherDayDate = dateTime.getYear() + "-" + dateTime.getMonthOfYear() + "-" + dateTime.getDayOfMonth();
		
		writer.write(getTweet(today, "h1", "l1") + "\n");
		writer.write(getTweet(today, "h1", "l1") + "\n");
		writer.write(getTweet(today, "h2", "l1") + "\n");
		writer.write(getTweet(today, "h2", "l1") + "\n");
		writer.write(getTweet(today, "h1", "l1") + "\n");
		writer.write(getTweet(today, "h3", "l2") + "\n");
		writer.write(getTweet(today, "h4", "l2") + "\n");
		writer.write(getTweet(today, "h4", "l2") + "\n");
		writer.write(getTweet(today, "h4", "l2") + "\n");
		writer.write(getTweet(today, "h4", "l2") + "\n");
		writer.write(getTweet(today, "h1", "l1") + "\n");
		writer.write(getTweet(anotherDay, "h2", "l1") + "\n");
		writer.write(getTweet(anotherDay, "h2", "l1") + "\n");
		writer.write(getTweet(anotherDay, "h1", "l1") + "\n");
		writer.write(getTweet(anotherDay, "h4", "l2") + "\n");
		writer.write(getTweet(anotherDay, "h3", "l2") + "\n");
		writer.write(getTweet(anotherDay, "h3", "l2") + "\n");
		writer.close();
		
		ToolRunner.run(new TopNHashTags(), new String[] { INPUT, OUTPUT, "1" });
		
		int validatedOutputLines = 0;
		
		for(String line: Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"))) {
			String[] fields = line.split("\t");
			if(fields[0].equals("l1")) {
				if(fields[1].equals(todayDate)) {
					assertEquals("h1", fields[2]);
					validatedOutputLines++;
				} else if(fields[1].equals(anotherDayDate)) {
					assertEquals("h2", fields[2]);
					validatedOutputLines++;
				}
			} else if(fields[0].equals("l2")) {
				if(fields[1].equals(todayDate)) {
					assertEquals(fields[2], "h4");
					validatedOutputLines++;
				} else if(fields[1].equals(anotherDayDate)) {					
					assertEquals(fields[2], "h3");
					validatedOutputLines++;
				}
			}
		}
		
		assertEquals(4, validatedOutputLines);
		
		trash(INPUT, OUTPUT);
	}
	
	public String getTweet(long date, String hashTag, String location) throws ParseException, JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		SimpleTweet tweet = new SimpleTweet();
		tweet.setCreated_at(SimpleTweet.dateFormat.format(new Date(date)));
		tweet.setEntities(new Entities());
		tweet.getEntities().setHashtags(new ArrayList<HashTag>());
		tweet.getEntities().getHashtags().add(new HashTag());
		tweet.getEntities().getHashtags().get(0).setText(hashTag);
		tweet.setUser(new UserInfo());
		tweet.getUser().setLocation(location);
		return mapper.writeValueAsString(tweet);
	}
}
