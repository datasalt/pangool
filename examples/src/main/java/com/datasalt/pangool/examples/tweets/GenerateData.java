package com.datasalt.pangool.examples.tweets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GenerateData {

	public static void main(String[] args) throws IOException {
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

		for(int i = 0; i < dates.size(); i++) {
			
		}
		
		writer.close();
	}
}
