package com.datasalt.pangool.examples.tweets;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class Beans {

	public static class HashTag {
		String text;

		public String getText() {
			return text;
		}

		public void setText(String text) {
			this.text = text;
		}

	}

	public static class Entities {
		List<HashTag> hashtags;

		public List<HashTag> getHashtags() {
    	return hashtags;
    }

		public void setHashtags(List<HashTag> hashtags) {
    	this.hashtags = hashtags;
    }

	}

	public static class UserInfo {
		String location;

		public String getLocation() {
			return location;
		}

		public void setLocation(String location) {
			this.location = location;
		}

	}

	public static class SimpleTweet {
		String created_at;
		Date created_at_date;
		Entities entities;
		UserInfo user;
		
		public static SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);

		public String getCreated_at() {
			return created_at;
		}

		public void setCreated_at(String created_at) throws ParseException {
			this.created_at = created_at;
			created_at_date = dateFormat.parse(created_at);
		}

		public Date getCreated_at_date() {
    	return created_at_date;
    }

		public Entities getEntities() {
			return entities;
		}

		public void setEntities(Entities entities) {
			this.entities = entities;
		}

		public UserInfo getUser() {
			return user;
		}

		public void setUser(UserInfo user) {
			this.user = user;
		}

	}
}
