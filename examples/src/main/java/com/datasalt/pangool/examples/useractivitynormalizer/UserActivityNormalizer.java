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
package com.datasalt.pangool.examples.useractivitynormalizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.TupleRollupReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

/**
 * In this advanced example we are normalizing user activity on certain features. We have a register of ["user",
 * "feature", "clicks"] and we want to emit the normalized activity of the user towards each feature.
 * <p>
 * That is, if we have: <br>
 * ["user1", "feature1", 10] <br>
 * ["user1", "feature1", 5] <br>
 * ["user1", "feature1", 20] <br>
 * ["user1", "feature2", 25] <br>
 * <br>
 * We want to have as output: <br>
 * <br>
 * ["user1", "feature1", 35 / 60] <-- Because 35 is the total clicks of user1 for feature1 and 60 the total clicks for
 * user1, overall <br>
 * ["user1", "feature2", 25 / 25] <-- Because 25 is the total clicks of user1 for feature1 and the total clicks for
 * user2, overall <br>
 * <br>
 * <p>
 * We have to sum up all the clicks per feature. But we need the total number of clicks before processing each feature.
 * <p>
 * For that purpose, we will create a intermediate Pangool schema ["user", "all", "feature", "clicks"] with a special
 * field called "all" that we will sort by. If all = true, the associated clicks will mean global clicks. This way, for
 * each user, we will have the global count of clicks first and the individual counts per feature afterwards.
 * <p>
 * We will group by ["user", "all", "feature"]. However, we want to process all features for the same user in the same
 * Reducer. For that purpose we will use rollupFrom("user"). Rollup will notify us when each new "user" opens so we can
 * reset the global clicks counter to 0.
 * <p>
 * This advanced use case includes a Combiner for reducing the intermediate input size that just sums up individual
 * feature counts.
 **/
public class UserActivityNormalizer extends BaseExampleJob {

	@SuppressWarnings("serial")
	private static class UserActivityProcessor extends TupleMapper<LongWritable, Text> {

		private Tuple tuple;

		public void setup(TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {
			this.tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("my_schema"));
		}

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {

			String[] fields = value.toString().trim().split("\t");
			tuple.set("user", fields[0]);
			tuple.set("feature", fields[1]);
			tuple.set("all", false);
			tuple.set("clicks", Integer.parseInt(fields[2]));
			collector.write(tuple);

			tuple.set("feature", "");
			tuple.set("all", true); // Emit another Tuple for "ALL" features.
			collector.write(tuple);
		}
	}

	/**
	 * This Combiner reduces the size of the intermediate output by aggregating clicks for each feature. It is the same
	 * idea than that of the WordCount Combiner.
	 */
	@SuppressWarnings("serial")
	public static class CountCombinerHandler extends TupleReducer<ITuple, NullWritable> {

		private Tuple tuple;

		public void setup(TupleMRContext context, Collector collector) throws IOException,
		    InterruptedException {
			tuple = new Tuple(context.getTupleMRConfig().getIntermediateSchema("my_schema"));
		}

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			int featureClicks = 0;
			// Sum total clicks for this feature
			for(ITuple tuple : tuples) {
				featureClicks += (Integer) tuple.get("clicks");
			}
			tuple.set("user", group.get("user"));
			tuple.set("feature", group.get("feature"));
			tuple.set("all", group.get("all"));
			tuple.set("clicks", featureClicks);
			collector.write(tuple, NullWritable.get());
		}
	}

	/**
	 * Because we are sorting by "all", "feature", for each "user" we will receive the "all" counts first. We can check
	 * the tuple "all" field for that and save the total clicks in a variable. Then we can normalize the total clicks for
	 * each individual feature.
	 */
	@SuppressWarnings("serial")
	public static class NormalizingHandler extends TupleRollupReducer<Text, NullWritable> {

		int totalClicks;

		public void onOpenGroup(int depth, String field, ITuple firstElement, TupleMRContext context,
		    Collector collector) throws IOException, InterruptedException, TupleMRException {

			if(field.equals("user")) { // New user: reset count
				totalClicks = 0;
			}
		};

		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			int featureClicks = 0;
			// Sum total clicks for this feature
			for(ITuple tuple : tuples) {
				featureClicks += (Integer) tuple.get("clicks");
			}

			boolean all = (Boolean) group.get("all");
			// If tuple has all == true, we are gathering total clicks for all features. This happens beginning of each group
			// because we sort by "all" field.
			if(all) {
				totalClicks += featureClicks;
				return;
			}

			// Otherwise we can normalize the clicks for this feature because we already aggregated the total clicks
			double normalizedActivity = featureClicks / (double) totalClicks;
			collector.write(new Text(group.get("user") + "\t" + group.get("feature") + "\t"
			    + normalizedActivity), NullWritable.get());
		};
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Wrong number of arguments");
			return -1;
		}
		String input = args[0];
		String output = args[1];

		delete(output);

		// Configure schema, sort and group by
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("user", Type.STRING));
		fields.add(Field.create("feature", Type.STRING));
		fields.add(Field.create("all", Type.BOOLEAN));
		fields.add(Field.create("clicks", Type.INT));

		Schema schema = new Schema("my_schema", fields);

		TupleMRBuilder mr = new TupleMRBuilder(conf);
		mr.addIntermediateSchema(schema);
		mr.setGroupByFields("user", "all", "feature");
		mr.setOrderBy(new OrderBy().add("user", Order.ASC).add("all", Order.DESC).add("feature", Order.ASC));
		// Rollup from "user" - all features from same user will go to the same Reducer
		mr.setRollupFrom("user");
		// Input / output and such
		mr.setTupleCombiner(new CountCombinerHandler());
		mr.setTupleReducer(new NormalizingHandler());
		mr.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class,
		    NullWritable.class);
		mr.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class),
		    new UserActivityProcessor());

		try {
			mr.createJob().waitForCompletion(true);
		} finally {
			mr.cleanUpInstanceFiles();
		}
		return 1;
	}

	public UserActivityNormalizer() {
		super("Usage: [input_path] [output_path]");
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new UserActivityNormalizer(), args);
	}
}
