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
package com.datasalt.pangool.examples.topicalwordcount;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.examples.topicalwordcount.TopicalWordCount.CountReducer;
import com.datasalt.pangool.examples.topicalwordcount.TopicalWordCount.TokenizeMapper;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.google.common.io.Files;

/**
 * Slightly modified version of {@link TopicalWordCount} that accepts a list of stop words.
 * This example shows how easy it is to manage trivial state in mapper and reducers with Pangool. 
 */
public class TopicalWordCountWithStopWords extends BaseExampleJob {

	@SuppressWarnings("serial")
	public static class StopWordMapper extends TokenizeMapper {

		private Set<String> stopWords = new HashSet<String>();
		
		public StopWordMapper(List<String> stopWords) {
			this.stopWords.addAll(stopWords);
			this.stopWords = Collections.unmodifiableSet(this.stopWords);
		}

    @Override
    protected void emitTuple(Collector collector)
        throws IOException, InterruptedException {
    	// Perform stop word filtering here
    	if(stopWords.contains(tuple.get("word"))) {
    		return;
    	}
    	super.emitTuple(collector);
    }
	}
	
	public TopicalWordCountWithStopWords() {
		super("Usage: TopicalWordCountWithStopWords [input_path] [output_path] [stop_word_list]");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			failArguments("Wrong number of arguments");
			return -1;
		}

		delete(args[1]);
		List<String> stopWords = Files.readLines(new File(args[2]), Charset.forName("UTF-8"));

		TupleMRBuilder cg = new TupleMRBuilder(conf, "Pangool Topical Word Count With Stop Words");
		cg.addIntermediateSchema(TopicalWordCount.getSchema());
		// We will count each (topicId, word) pair
		// Note that the order in which we defined the fields of the Schema is not relevant here
		cg.setGroupByFields("topic", "word");
		// Here we instantiate a mapper with stop words:
		// Note that we don't need to use the DistributedCache for that becasuse mappers, reducers, etc themselves are instantiable
		StopWordMapper mapper = new StopWordMapper(stopWords);
		cg.addInput(new Path(args[0]), new HadoopInputFormat(TextInputFormat.class), mapper);
		// We'll use a TupleOutputFormat with the same schema than the intermediate schema
		cg.setTupleOutput(new Path(args[1]), TopicalWordCount.getSchema());
		cg.setTupleReducer(new CountReducer());
		cg.setTupleCombiner(new CountReducer());

		cg.createJob().waitForCompletion(true);

		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TopicalWordCountWithStopWords(), args);
	}
}
