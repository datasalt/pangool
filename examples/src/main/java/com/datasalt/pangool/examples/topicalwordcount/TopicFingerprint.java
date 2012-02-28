package com.datasalt.pangool.examples.topicalwordcount;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.cogroup.TupleMRBuilder;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.processors.IdentityTupleMapper;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.tuple.ITuple;

public class TopicFingerprint extends BaseExampleJob {

	@SuppressWarnings("serial")
  public static class TopNWords extends TupleReducer<ITuple, NullWritable> {

		int n; // We will emit as many words as this parameter (at maximum) for each group

		public TopNWords(int n) {
			this.n = n;
		}

		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext coGrouperContext, Collector collector)
		    throws java.io.IOException, InterruptedException, TupleMRException {

			Iterator<ITuple> iterator = tuples.iterator();
			for(int i = 0; i < n && iterator.hasNext(); i++) {
				collector.write(iterator.next(), NullWritable.get());
			}
		};
	}
	
	public TopicFingerprint() {
		super("Usage: TopicalWordCountWithStopWords [input_path] [output_path] [top_n_size]");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			failArguments("Wrong number of arguments");
		}

		deleteOuput(args[1]);
		// Parse the size of the Top
		Integer n = Integer.parseInt(args[2]);
		
		TupleMRBuilder cg = new TupleMRBuilder(conf, "Pangool Topic Fingerprint From Topical Word Count");
		cg.addIntermediateSchema(TopicalWordCount.getSchema());
		// We need to group the counts by (topic)
		cg.setGroupByFields("topic");
		// Then we need to sort by topic and count (DESC) -> This way we will receive the most relevant words first.
		cg.setOrderBy(new SortBy().add("topic", Order.ASC).add("count", Order.DESC));
		// Note that we are changing the grouping logic in the job configuration,
		// However, as we work with tuples, we don't need to write specific code for grouping the same data differently,
		// Therefore an IdentityTupleMapper is sufficient for this Job.
		cg.addTupleInput(new Path(args[0]), new IdentityTupleMapper()); // Note the use of "addTupleInput"
		/*
		 * TODO Add Combiner as same Reducer when possible
		 */
		cg.setTupleOutput(new Path(args[1]), TopicalWordCount.getSchema());
		cg.setTupleReducer(new TopNWords(n));
		
		cg.createJob().waitForCompletion(true);

		return 1;
	}
}
