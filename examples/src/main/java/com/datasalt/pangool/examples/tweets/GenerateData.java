package com.datasalt.pangool.examples.tweets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.examples.TestUtils;
import com.datasalt.pangool.examples.tweets.Beans.Entities;
import com.datasalt.pangool.examples.tweets.Beans.HashTag;
import com.datasalt.pangool.examples.tweets.Beans.SimpleTweet;
import com.datasalt.pangool.examples.tweets.Beans.UserInfo;

public class GenerateData {

	public static void main(String[] args) throws IOException, ParseException {
		if(args.length != 5) {
			System.err.println();
			System.err.println("Five arguments are needed.");
			System.err
			    .println("Usage: [out-tweets-file] [nTweets] [nHashtags] [nLocations] [nDates]");
			System.err.println();
			System.err
			    .println("Example: tweets.txt 100 10 10 10 -> Will generate a file 'tweets.txt' with 100 tweets. There will be 10 different hashtags, 10 different locations and 10 different dates used among all generated tweets.");
			System.err.println();
			System.exit(-1);
		}
		String outFile = args[0];
		int nTweets = Integer.parseInt(args[1]);
		
		List<String> hashTags = new ArrayList<String>(Integer.parseInt(args[2]));
		List<String> locations = new ArrayList<String>(Integer.parseInt(args[3]));
		List<Long> dates = new ArrayList<Long>(Integer.parseInt(args[4]));
		
		// Pregenerate data that will be used to generate tweets
		
		for(int i = 0; i < hashTags.size(); i++) {
			hashTags.add("hashtag" + TestUtils.randomString(10));
		}
		
		for(int i = 0; i < locations.size(); i++) {
			locations.add("location" + TestUtils.randomString(10));
		}

		long currDate = System.currentTimeMillis();
		for(int i = 0; i < dates.size(); i++) {
			dates.add(currDate);
			currDate -= 1000 * 60 * 60 * 24;
		}

		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		ObjectMapper mapper = new ObjectMapper();
		
		for(int i = 0; i < nTweets; i++) {
			long date = dates.get((int)(Math.random() * dates.size()));
			String location = locations.get((int)(Math.random() * locations.size()));
			String hashTag = hashTags.get((int)(Math.random() * hashTags.size()));
			SimpleTweet tweet = new SimpleTweet();
			tweet.setCreated_at(SimpleTweet.dateFormat.format(new Date(date)));
			tweet.setEntities(new Entities());
			tweet.getEntities().setHashtags(new ArrayList<HashTag>());
			tweet.getEntities().getHashtags().add(new HashTag());
			tweet.getEntities().getHashtags().get(0).setText(hashTag);
			tweet.setUser(new UserInfo());
			tweet.getUser().setLocation(location);
			writer.write(mapper.writeValueAsString(tweet) + "\n");
		}
		
		writer.close();
	}
}
