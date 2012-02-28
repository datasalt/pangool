package com.datasalt.pangool.examples.topicalwordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;

public class TopicFingerprint extends BaseExampleJob {

	public final static String OUTPUT_TOTALCOUNT = "totalcount";
	
	@SuppressWarnings("serial")
	public static class TopNWords extends TupleReducer<ITuple, NullWritable> {

		int n; // We will emit as many words as this parameter (at maximum) for each group
		Tuple outputCountTuple;

		public TopNWords(int n) {
			this.n = n;
		}

		public void setup(TupleMRContext coGrouperContext, Collector collector) throws IOException, InterruptedException,
		    TupleMRException {
			outputCountTuple = new Tuple(getOutputCountSchema());
		};

		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext coGrouperContext, Collector collector)
		    throws java.io.IOException, InterruptedException, TupleMRException {

			int totalCount = 0;
			Iterator<ITuple> iterator = tuples.iterator();
			for(int i = 0; i < n && iterator.hasNext(); i++) {
				ITuple tuple = iterator.next();
				collector.write(tuple, NullWritable.get());
				totalCount += (Integer) tuple.get("count");
			}
			
			outputCountTuple.set("topic", group.get("topic"));
			outputCountTuple.set("totalcount", totalCount);
			collector.getNamedOutput(OUTPUT_TOTALCOUNT).write(outputCountTuple, NullWritable.get());
		};
	}

	public TopicFingerprint() {
		super("Usage: TopicalWordCountWithStopWords [input_path] [output_path] [top_n_size]");
	}

	public static Schema getOutputCountSchema() {
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("topic", Type.INT));
		fields.add(Field.create("totalcount", Type.INT));
		return new Schema("outputcount", fields);
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
		cg.setOrderBy(new OrderBy().add("topic", Order.ASC).add("count", Order.DESC));
		// Note that we are changing the grouping logic in the job configuration,
		// However, as we work with tuples, we don't need to write specific code for grouping the same data differently,
		// Therefore an IdentityTupleMapper is sufficient for this Job.
		cg.addTupleInput(new Path(args[0]), new IdentityTupleMapper()); // Note the use of "addTupleInput"
		/*
		 * TODO Add Combiner as same Reducer when possible
		 */
		cg.setTupleOutput(new Path(args[1]), TopicalWordCount.getSchema());
		cg.addNamedTupleOutput(OUTPUT_TOTALCOUNT, getOutputCountSchema());
		cg.setTupleReducer(new TopNWords(n));

		cg.createJob().waitForCompletion(true);

		return 1;
	}
}
