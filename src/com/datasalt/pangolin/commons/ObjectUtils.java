package com.datasalt.pangolin.commons;

import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Class with utilities for Objects in general.
 * 
 * @author ivan
 */
public class ObjectUtils {

	private static final ObjectMapper JSON_MAPPER = new ObjectMapper(new JsonFactory());
	private static final ObjectMapper INDENT_JSON_MAPPER = new ObjectMapper(new JsonFactory());
	static {
		INDENT_JSON_MAPPER.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
		
		INDENT_JSON_MAPPER.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
		JSON_MAPPER.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
	}

	/**
	 * Tries to serialize the given object as a JSON.
	 * The JSON is pretty formatted.
	 */
	public static String toIndentedJSON(Object object)  {
		try {
	    return INDENT_JSON_MAPPER.writeValueAsString(object);
    } catch (JsonGenerationException e) {
	    e.printStackTrace();
    } catch (JsonMappingException e) {
	    e.printStackTrace();
    } catch (IOException e) {
	    e.printStackTrace();
    }
    return "";
	}

	/**
	 * Tries to serialize the given object as a JSON.
	 */
	public static String toJSON(Object object)  {
		try {
	    return JSON_MAPPER.writeValueAsString(object);
    } catch (JsonGenerationException e) {
	    e.printStackTrace();
    } catch (JsonMappingException e) {
	    e.printStackTrace();
    } catch (IOException e) {
	    e.printStackTrace();
    }
    return "";
	}

	
//	private static String t = "{\"place\":null,\"in_reply_to_user_id\":252992023,\"user\":{\"notifications\":null,\"friends_count\":102,\"profile_text_color\":\"333333\",\"protected\":false,\"is_translator\":false,\"profile_sidebar_fill_color\":\"DDEEF6\",\"location\":\"\",\"name\":\"Iv\\u00e1n de Prado\",\"follow_request_sent\":null,\"statuses_count\":158,\"profile_background_tile\":false,\"utc_offset\":3600,\"url\":\"http:\\/\\/www.ivanprado.es\",\"id_str\":\"13623842\",\"following\":null,\"verified\":false,\"profile_background_image_url_https\":\"https:\\/\\/si0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"favourites_count\":0,\"profile_link_color\":\"0084B4\",\"description\":\"@datasalt co-founder\",\"created_at\":\"Mon Feb 18 14:09:50 +0000 2008\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_image_url_https\":\"https:\\/\\/si0.twimg.com\\/profile_images\\/1380084036\\/tw_8765996_1307103932_normal.jpg\",\"default_profile\":true,\"listed_count\":5,\"time_zone\":\"Madrid\",\"profile_image_url\":\"http:\\/\\/a3.twimg.com\\/profile_images\\/1380084036\\/tw_8765996_1307103932_normal.jpg\",\"contributors_enabled\":false,\"profile_use_background_image\":true,\"id\":13623842,\"profile_background_color\":\"C0DEED\",\"followers_count\":104,\"screen_name\":\"ivanprado\",\"profile_background_image_url\":\"http:\\/\\/a0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"default_profile_image\":false,\"show_all_inline_media\":false,\"lang\":\"en\",\"geo_enabled\":false},\"in_reply_to_status_id\":null,\"text\":\"@ferrerabertran tweet de test @LauriSanchez http:\\/\\/t.co\\/w73rlpK http:\\/\\/t.co\\/kfx4SnC #test #test2\",\"id_str\":\"92956033756053506\",\"favorited\":false,\"created_at\":\"Mon Jul 18 13:57:00 +0000 2011\",\"in_reply_to_status_id_str\":null,\"geo\":null,\"in_reply_to_screen_name\":\"ferrerabertran\",\"id\":92956033756053506,\"in_reply_to_user_id_str\":\"252992023\",\"source\":\"web\",\"contributors\":null,\"coordinates\":null,\"retweeted\":false,\"retweet_count\":0,\"truncated\":false,\"entities\":{\"user_mentions\":[{\"name\":\"Pere Ferrera Bertran\",\"id_str\":\"252992023\",\"indices\":[0,15],\"id\":252992023,\"screen_name\":\"ferrerabertran\"},{\"name\":\"Laura S\\u00e1nchez\",\"id_str\":\"48471323\",\"indices\":[30,43],\"id\":48471323,\"screen_name\":\"LauriSanchez\"}],\"hashtags\":[{\"text\":\"test\",\"indices\":[84,89]},{\"text\":\"test2\",\"indices\":[90,96]}],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/w73rlpK\",\"display_url\":\"datasalt.com\",\"indices\":[44,63],\"expanded_url\":\"http:\\/\\/www.datasalt.com\\/\"},{\"url\":\"http:\\/\\/t.co\\/kfx4SnC\",\"display_url\":\"datasalt.com\\/blog\",\"indices\":[64,83],\"expanded_url\":\"http:\\/\\/www.datasalt.com\\/blog\"}]}}";
//	private static String nativeRetweet = "{\"contributors\":null,\"place\":null,\"retweeted\":false,\"retweet_count\":1,\"user\":{\"notifications\":null,\"profile_background_image_url\":\"http:\\/\\/a0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"protected\":false,\"default_profile\":true,\"location\":\"\",\"name\":\"Iv\\u00e1n de Prado\",\"profile_text_color\":\"333333\",\"show_all_inline_media\":false,\"listed_count\":5,\"contributors_enabled\":false,\"geo_enabled\":true,\"profile_background_image_url_https\":\"https:\\/\\/si0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"utc_offset\":3600,\"profile_sidebar_fill_color\":\"DDEEF6\",\"url\":\"http:\\/\\/www.ivanprado.es\",\"following\":null,\"verified\":false,\"profile_background_tile\":false,\"profile_image_url_https\":\"https:\\/\\/si0.twimg.com\\/profile_images\\/1380084036\\/tw_8765996_1307103932_normal.jpg\",\"description\":\"@datasalt co-founder\",\"created_at\":\"Mon Feb 18 14:09:50 +0000 2008\",\"id_str\":\"13623842\",\"statuses_count\":159,\"favourites_count\":0,\"profile_link_color\":\"0084B4\",\"profile_image_url\":\"http:\\/\\/a3.twimg.com\\/profile_images\\/1380084036\\/tw_8765996_1307103932_normal.jpg\",\"is_translator\":false,\"default_profile_image\":false,\"profile_sidebar_border_color\":\"C0DEED\",\"id\":13623842,\"time_zone\":\"Madrid\",\"friends_count\":102,\"followers_count\":104,\"screen_name\":\"ivanprado\",\"profile_use_background_image\":true,\"follow_request_sent\":null,\"lang\":\"en\",\"profile_background_color\":\"C0DEED\"},\"in_reply_to_status_id\":null,\"retweeted_status\":{\"contributors\":null,\"place\":null,\"retweeted\":false,\"retweet_count\":1,\"user\":{\"notifications\":null,\"profile_background_image_url\":\"http:\\/\\/a0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"protected\":false,\"default_profile\":true,\"location\":\"Corvallis, OR\",\"name\":\"BigML\",\"profile_text_color\":\"333333\",\"show_all_inline_media\":false,\"listed_count\":12,\"contributors_enabled\":false,\"geo_enabled\":false,\"profile_background_image_url_https\":\"https:\\/\\/si0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"utc_offset\":null,\"profile_sidebar_fill_color\":\"DDEEF6\",\"url\":\"http:\\/\\/bigml.com\",\"following\":null,\"verified\":false,\"profile_background_tile\":false,\"profile_image_url_https\":\"https:\\/\\/si0.twimg.com\\/profile_images\\/1276655904\\/BigML-logo-vierkant1_normal.jpg\",\"description\":\"Machine Learning, made simple\",\"created_at\":\"Sun Jan 09 04:33:15 +0000 2011\",\"id_str\":\"235821180\",\"statuses_count\":169,\"favourites_count\":0,\"profile_link_color\":\"0084B4\",\"profile_image_url\":\"http:\\/\\/a2.twimg.com\\/profile_images\\/1276655904\\/BigML-logo-vierkant1_normal.jpg\",\"is_translator\":false,\"default_profile_image\":false,\"profile_sidebar_border_color\":\"C0DEED\",\"id\":235821180,\"time_zone\":null,\"friends_count\":375,\"followers_count\":230,\"screen_name\":\"bigmlcom\",\"profile_use_background_image\":true,\"follow_request_sent\":null,\"lang\":\"en\",\"profile_background_color\":\"C0DEED\"},\"in_reply_to_status_id\":null,\"in_reply_to_user_id\":null,\"text\":\"http:\\/\\/t.co\\/Juszvae via @flowingdata Great viz of where peope upload Flickr photo's and tweet. By Eric Fisher.\",\"created_at\":\"Mon Jul 18 13:26:53 +0000 2011\",\"id_str\":\"92948454388678657\",\"geo\":null,\"favorited\":false,\"id\":92948454388678657,\"in_reply_to_status_id_str\":null,\"source\":\"\\u003Ca href=\\\"http:\\/\\/twitter.com\\/tweetbutton\\\" rel=\\\"nofollow\\\"\\u003ETweet Button\\u003C\\/a\\u003E\",\"coordinates\":null,\"in_reply_to_screen_name\":null,\"truncated\":false,\"in_reply_to_user_id_str\":null,\"entities\":{\"hashtags\":[],\"user_mentions\":[{\"name\":\"Nathan Yau\",\"indices\":[24,36],\"id_str\":\"14109167\",\"id\":14109167,\"screen_name\":\"flowingdata\"}],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/Juszvae\",\"indices\":[0,19],\"display_url\":\"flowingdata.com\\/2011\\/07\\/12\\/fli\\u2026\",\"expanded_url\":\"http:\\/\\/flowingdata.com\\/2011\\/07\\/12\\/flickr-and-twitter-mapped-together-see-something-or-say-something\\/\"}]}},\"in_reply_to_user_id\":null,\"text\":\"RT @bigmlcom: http:\\/\\/t.co\\/Juszvae via @flowingdata Great viz of where peope upload Flickr photo's and tweet. By Eric Fisher.\",\"created_at\":\"Mon Jul 18 17:22:42 +0000 2011\",\"id_str\":\"93007802225803265\",\"geo\":null,\"favorited\":false,\"id\":93007802225803265,\"in_reply_to_status_id_str\":null,\"source\":\"web\",\"coordinates\":null,\"in_reply_to_screen_name\":null,\"truncated\":false,\"in_reply_to_user_id_str\":null,\"entities\":{\"hashtags\":[],\"user_mentions\":[{\"name\":\"BigML\",\"indices\":[3,12],\"id_str\":\"235821180\",\"id\":235821180,\"screen_name\":\"bigmlcom\"},{\"name\":\"Nathan Yau\",\"indices\":[38,50],\"id_str\":\"14109167\",\"id\":14109167,\"screen_name\":\"flowingdata\"}],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/Juszvae\",\"indices\":[14,33],\"display_url\":\"flowingdata.com\\/2011\\/07\\/12\\/fli\\u2026\",\"expanded_url\":\"http:\\/\\/flowingdata.com\\/2011\\/07\\/12\\/flickr-and-twitter-mapped-together-see-something-or-say-something\\/\"}]}}";
//	private static String manualRetweet = "{\"place\":null,\"in_reply_to_user_id\":null,\"user\":{\"is_translator\":false,\"default_profile_image\":false,\"notifications\":false,\"profile_text_color\":\"333333\",\"protected\":false,\"show_all_inline_media\":false,\"geo_enabled\":true,\"friends_count\":102,\"profile_sidebar_fill_color\":\"DDEEF6\",\"location\":\"\",\"name\":\"Iv\\u00e1n de Prado\",\"profile_background_tile\":false,\"follow_request_sent\":false,\"utc_offset\":3600,\"url\":\"http:\\/\\/www.ivanprado.es\",\"id_str\":\"13623842\",\"default_profile\":true,\"statuses_count\":160,\"following\":true,\"verified\":false,\"favourites_count\":0,\"profile_link_color\":\"0084B4\",\"description\":\"@datasalt co-founder\",\"created_at\":\"Mon Feb 18 14:09:50 +0000 2008\",\"profile_sidebar_border_color\":\"C0DEED\",\"time_zone\":\"Madrid\",\"profile_image_url\":\"http:\\/\\/a3.twimg.com\\/profile_images\\/1380084036\\/tw_8765996_1307103932_normal.jpg\",\"listed_count\":5,\"contributors_enabled\":false,\"profile_use_background_image\":true,\"profile_background_image_url_https\":\"https:\\/\\/si0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"id\":13623842,\"profile_background_color\":\"C0DEED\",\"followers_count\":104,\"profile_image_url_https\":\"https:\\/\\/si0.twimg.com\\/profile_images\\/1380084036\\/tw_8765996_1307103932_normal.jpg\",\"screen_name\":\"ivanprado\",\"profile_background_image_url\":\"http:\\/\\/a0.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"lang\":\"en\"},\"in_reply_to_status_id\":null,\"text\":\"RT @Milhaud: Con los datos sobre la mesa, el c\\u00e1ncer causa claramente el uso de tel\\u00e9fono m\\u00f3vil: http:\\/\\/t.co\\/BlTD98O\",\"id_str\":\"93014395789127680\",\"favorited\":false,\"created_at\":\"Mon Jul 18 17:48:54 +0000 2011\",\"in_reply_to_status_id_str\":null,\"geo\":null,\"in_reply_to_screen_name\":null,\"id\":93014395789127680,\"in_reply_to_user_id_str\":null,\"source\":\"web\",\"contributors\":null,\"coordinates\":null,\"retweeted\":false,\"retweet_count\":0,\"truncated\":false,\"entities\":{\"hashtags\":[],\"user_mentions\":[{\"name\":\"Miguel Garc\\u00eda\",\"id_str\":\"18690355\",\"indices\":[3,11],\"id\":18690355,\"screen_name\":\"Milhaud\"}],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/BlTD98O\",\"display_url\":\"bit.ly\\/oWiP6G\",\"indices\":[95,114],\"expanded_url\":\"http:\\/\\/bit.ly\\/oWiP6G\"}]}}";
//	
//	/*
//	 * for JSON parsing
//	 */
//	private final static TypeReference<HashMap<String, Object>> listTypeRef = new TypeReference<HashMap<String, Object>>() { }; 

//	public static void main (String args[]) throws JsonParseException, JsonMappingException, IOException, ParseException{
//		ObjectMapper mapper = new ObjectMapper(new JsonFactory());;
//		//HashMap<String, Object>	tweetList = mapper.readValue(t, listTypeRef);
//		//System.out.println(Objects.toStringHelper(tweetList));
//		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//	
//		
//		Tweet tweet = mapper.readValue(t, Tweet.class);
//		TweetParser parser = new TweetParser();
//		tweet = parser.parse(nativeRetweet);
//		mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
//		System.out.println(toIndentedJSON(tweet));
//	}
}
