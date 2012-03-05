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
package com.datasalt.pangool.examples;

import org.apache.hadoop.util.ProgramDriver;

import com.datasalt.pangool.examples.movingaverage.MovingAverage;
import com.datasalt.pangool.examples.movingaverage.MovingAverageGenerateData;
import com.datasalt.pangool.examples.secondarysort.SecondarySort;
import com.datasalt.pangool.examples.secondarysort.SecondarySortGenerateData;
import com.datasalt.pangool.examples.simplesecondarysort.SimpleSecondarySort;
import com.datasalt.pangool.examples.simplesecondarysort.SimpleSecondarySortGenerateData;
import com.datasalt.pangool.examples.topicalwordcount.TopicFingerprint;
import com.datasalt.pangool.examples.topicalwordcount.TopicalWordCount;
import com.datasalt.pangool.examples.topicalwordcount.TopicalWordCountGenerateData;
import com.datasalt.pangool.examples.topicalwordcount.TopicalWordCountWithStopWords;
import com.datasalt.pangool.examples.topnhashtags.TopNHashTags;
import com.datasalt.pangool.examples.topnhashtags.TopNHashTagsGenerateData;
import com.datasalt.pangool.examples.urlresolution.UrlResolution;
import com.datasalt.pangool.examples.urlresolution.UrlResolutionGenerateData;
import com.datasalt.pangool.examples.useractivitynormalizer.UserActivityNormalizer;
import com.datasalt.pangool.examples.useractivitynormalizer.UserActivityNormalizerGenerateData;

public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		super();
		addClass("tupleviewer", TupleViewer.class, "A tool for textualizing tuple outputs. You give it a glob or path and it will print the stringified tuples.");
		addClass("grep", Grep.class, "Map-only job that performs a grep");
		addClass("simple_secondarysort", SimpleSecondarySort.class, "Typical simple secondary sort (two ints) in Pangool.");
		addClass("simple_secondarysort_gen_data", SimpleSecondarySortGenerateData.class, "Input data generator for the simple secondary sort.");
		addClass("secondarysort", SecondarySort.class, "A more advanced secondary sort example.");
		addClass("secondarysort_gen_data", SecondarySortGenerateData.class, "Input data generator for the secondary sort example.");
		addClass("topical_word_count_gen_data", TopicalWordCountGenerateData.class, "Input data generator for the topical word count example.");
		addClass("topical_word_count", TopicalWordCount.class, "The topical word count example from Pangool's introduction.");
		addClass("topical_word_count_with_stop_words", TopicalWordCountWithStopWords.class, "The topical word count example from Pangool's introduction extended to accept a list of stop words.");
		addClass("topical_word_count_topic_fingerprint", TopicFingerprint.class, "The topic fingerprint Job from the topical word count example in Pangool's introduction.");
		addClass("top_n_hashtags", TopNHashTags.class, "Rollup example that calculates the top N hashtags in each (location, date) from a tweets dataset.");
		addClass("top_n_hashtags_gen_data", TopNHashTagsGenerateData.class, "Input data generator for the top N hashtags.");
		addClass("moving_average_gen_data", MovingAverageGenerateData.class, "Input data generator for the Moving average.");
		addClass("moving_average", MovingAverage.class, "Moving average example. Calculates the n-window size moving average from a dataset of [url, date, clicks].");
		addClass("moving_average_gen_data", MovingAverageGenerateData.class, "Input data generator for the Moving average.");
		addClass("url_resolution", UrlResolution.class, "Reduce-side join example. Url Resolution. Cogroup url registers (url, date, ip) with url mappings (url, cannonical_url).");
		addClass("url_resolution_gen_data", UrlResolutionGenerateData.class, "Input data generator for the Url Resolution.");
		addClass("user_activity_normalizer", UserActivityNormalizer.class, "Rollup example that emits the normalized activity of users over features. It emits the feature_clicks / total_clicks from a [user, feature, clicks] dataset.");
		addClass("user_activity_normalizer_gen_data", UserActivityNormalizerGenerateData.class, "Input data generator for the user activity normalizer.");
	}

	public static void main(String[] args) throws Throwable {
		Driver driver = new Driver();
		driver.driver(args);
		System.exit(0);
	}
}
