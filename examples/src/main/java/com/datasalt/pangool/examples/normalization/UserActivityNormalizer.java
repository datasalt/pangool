package com.datasalt.pangool.examples.normalization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.datasalt.pangool.cogroup.CoGrouper;
import com.datasalt.pangool.cogroup.CoGrouperException;
import com.datasalt.pangool.cogroup.processors.CombinerHandler;
import com.datasalt.pangool.cogroup.processors.GroupHandlerWithRollup;
import com.datasalt.pangool.cogroup.processors.InputProcessor;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Tuple;

/**
 * In this example we are normalizing user activity on certain features. We have a register of ["user", "feature",
 * "clicks"] and we want to emit the normalized activity of the user towards each feature.
 * <p>
 * We may have more than one register per each "user", "feature", so we have to sum up all the clicks per feature. But
 * we also want to normalize them by the total number of clicks. For that purpose, we will create a intermediate Pangool
 * schema with a special field called "all" that we will sort (but not group) by. This way, we will receive the counts
 * for all features first for each user group so we will be able to keep them in memory.
 * <p>
 * We will group by "user", "all", "feature". However, we want to receive all features to the same Reducer. For this we
 * will use .setCustomPartitionFields("user");
 * <p>
 * This advanced use case includes a Combiner for reducing the intermediate input size that just sums up individual
 * feature counts.
 **/
public class UserActivityNormalizer {

	@SuppressWarnings("serial")
	private static class UserActivityProcessor extends InputProcessor<LongWritable, Text> {

		private Tuple tuple;

		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			this.tuple = new Tuple(context.getCoGrouperConfig().getSourceSchema("my_schema"));
		}

		@Override
		public void process(LongWritable key, Text value, CoGrouperContext context, Collector collector)
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
	public static class CountCombinerHandler extends CombinerHandler {

		private Tuple tuple;

		public void setup(CoGrouperContext context, Collector collector) throws IOException, InterruptedException {
			tuple = new Tuple(context.getCoGrouperConfig().getSourceSchema("my_schema"));
		}

		@Override
		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {

			int featureClicks = 0;
			// Sum total clicks for this feature
			for(ITuple tuple : tuples) {
				featureClicks += (Integer) tuple.get("clicks");
			}
			tuple.set("user", group.get("user"));
			tuple.set("feature", group.get("feature"));
			tuple.set("all", group.get("all"));
			tuple.set("clicks", featureClicks);
			collector.write(tuple);
		}
	}

	/**
	 * Because we are sorting by "all", "feature", for each "user" we will receive the "all" counts first. We can check
	 * the tuple "all" field for that and save the total clicks in a variable. Then we can normalize the total clicks for
	 * each individual feature.
	 */
	@SuppressWarnings("serial")
	public static class NormalizingHandler extends GroupHandlerWithRollup<Text, NullWritable> {

		int totalClicks;

		public void onOpenGroup(int depth, String field, ITuple firstElement, CoGrouperContext context, Collector collector)
		    throws IOException, InterruptedException, CoGrouperException {
			
			if(field.equals("user")) { // New user: reset count 
				totalClicks = 0;
			}
		};

		public void onGroupElements(ITuple group, Iterable<ITuple> tuples, CoGrouperContext coGrouperContext,
		    Collector collector) throws IOException, InterruptedException, CoGrouperException {

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
			collector.write(new Text(group.get("user") + "\t" + group.get("feature") + "\t" + normalizedActivity),
			    NullWritable.get());
		};
	}

	public Job getJob(Configuration conf, String input, String output) throws CoGrouperException, IOException {
		// Configure schema, sort and group by
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("user", String.class));
		fields.add(new Field("feature", String.class));
		fields.add(new Field("all", Boolean.class));
		fields.add(new Field("clicks", Integer.class));

		Schema schema = new Schema("my_schema", fields);

		CoGrouper grouper = new CoGrouper(conf);
		grouper.addSourceSchema(schema);
		grouper.setGroupByFields("user", "all", "feature");
		grouper.setOrderBy(new SortBy().add("user", Order.ASC).add("all", Order.DESC).add("feature", Order.ASC));
		// By partitioning by "user" field we assure that all features go to the same Reducer
		grouper.setRollupFrom("user");
		// Input / output and such
		grouper.setCombinerHandler(new CountCombinerHandler());
		grouper.setGroupHandler(new NormalizingHandler());
		grouper.setOutput(new Path(output), TextOutputFormat.class, Text.class, NullWritable.class);
		grouper.addInput(new Path(input), TextInputFormat.class, new UserActivityProcessor());
		return grouper.createJob();
	}
}
