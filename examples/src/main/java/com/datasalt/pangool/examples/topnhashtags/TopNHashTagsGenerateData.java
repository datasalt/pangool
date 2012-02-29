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
package com.datasalt.pangool.examples.topnhashtags;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.examples.Utils;
import com.datasalt.pangool.examples.topnhashtags.Beans.Entities;
import com.datasalt.pangool.examples.topnhashtags.Beans.HashTag;
import com.datasalt.pangool.examples.topnhashtags.Beans.SimpleTweet;
import com.datasalt.pangool.examples.topnhashtags.Beans.UserInfo;

/**
 * Input data generator for the {@link TopNHashTags} example.
 */
public class TopNHashTagsGenerateData {

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
		int nTweets = Integer.parseInt(args[1]), nHashTags = Integer.parseInt(args[2]), nLocations = Integer.parseInt(args[3]),
		nDates = Integer.parseInt(args[4]);
		
		List<String> hashTags = new ArrayList<String>(nHashTags);
		List<String> locations = new ArrayList<String>(nLocations);
		List<Long> dates = new ArrayList<Long>(nDates);
		
		// Pregenerate data that will be used to generate tweets
		
		for(int i = 0; i < nHashTags; i++) {
			hashTags.add("hashtag" + Utils.randomString(10));
		}
		
		for(int i = 0; i < nLocations; i++) {
			locations.add("location" + Utils.randomString(10));
		}

		long currDate = System.currentTimeMillis();
		for(int i = 0; i < nDates; i++) {
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
