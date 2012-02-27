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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.datasalt.pangool.cogroup.TupleMRBuilder;
import com.datasalt.pangool.cogroup.TupleMRException;
import com.datasalt.pangool.cogroup.processors.TupleReducer;
import com.datasalt.pangool.cogroup.processors.TupleMapper;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.SortBy;
import com.datasalt.pangool.io.HadoopInputFormat;
import com.datasalt.pangool.io.HadoopOutputFormat;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.test.Pair;

/**
 * We have a register of unique visits per day per URL. In this example we want to calculate, for each URL, the moving
 * average for "n" days of unique visits for all the registers we have.
 * <p>
 * That is, if we have:
 * <p>
 * ["url1", "2011-10-28", 10] <br>
 * ["url1", "2011-10-29", 20] <br>
 * ["url1", "2011-10-30", 30] <br>
 * ["url1", "2011-10-31", 40] <br>
 * <p>
 * For moving averages of 3 days we want to have, as output: 
 * <p>
 * ["url1", "2011-10-28", 10] <br>
 * ["url1", "2011-10-29", (20 + 10) / 2 = 15.0] <br>
 * ["url1", "2011-10-30", (30 + 20 + 10) / 3 = 20.0] <br>
 * ["url1", "2011-10-31", (40 + 30 + 20) / 3 = 30.0] <br>
 * <p>
 * With Pangool we can define an intermediate schema like ["url", "date", "clicks"], group by "url" and sort by "url", "date".
 * Then we can just keep a windowed stack of data points and compute the average per each date that we receive.  
 */
public class MovingAverage {

	@SuppressWarnings("serial")
	private static class URLVisitsProcessor extends TupleMapper<LongWritable, Text> {

		private Schema schema;

		public void setup(TupleMRContext context, Collector collector) throws IOException, InterruptedException {
			this.schema = context.getTupleMRConfig().getIntermediateSchema("my_schema");
		}

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {

			// Just parsing the text input and emitting a Tuple
			Tuple tuple = new Tuple(schema);
			String[] fields = value.toString().trim().split("\t");
			tuple.set("url", fields[0]);
			tuple.set("date", fields[1]);
			tuple.set("visits", Integer.parseInt(fields[2]));
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class MovingAverageHandler extends TupleReducer<Text, NullWritable> {

		int nDaysAverage;
		Queue<Pair<Integer, DateTime>> dataPoints = new LinkedList<Pair<Integer, DateTime>>();
		private final static DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd");

		public MovingAverageHandler(int nDaysAverage) { // Configurable number of moving average days
			this.nDaysAverage = nDaysAverage;
		}

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			dataPoints.clear();

			for(ITuple tuple : tuples) {
				String currentDate = tuple.get("date").toString();
				DateTime date = format.parseDateTime(currentDate);
				// Add current data point to the window
				dataPoints.add(new Pair<Integer, DateTime>((Integer) tuple.get("visits"), date));
				// Adjust window to desired date
				DateTime desiredDate = date.plusDays(-(nDaysAverage - 1));
				Pair<Integer, DateTime> lastDataPoint = dataPoints.peek();
				while(lastDataPoint.getSecond().isBefore(desiredDate)) {
					dataPoints.poll();
					lastDataPoint = dataPoints.peek();
				}
				// Calculate current average and emit
				int average = 0;
				for(Pair<Integer, DateTime> dataPoint : dataPoints) {
					average += dataPoint.getFirst();
				}
				double avg = average / (double) dataPoints.size();
				collector.write(new Text(tuple.get("url") + "\t" + currentDate + "\t" + avg), NullWritable.get());
			}
		}
	}

	public Job getJob(Configuration conf, String input, String output, int nDaysAverage) throws TupleMRException,
	    IOException {
		// Configure schema, sort and group by
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("url", Utf8.class));
		fields.add(new Field("date", Utf8.class));
		fields.add(new Field("visits", Integer.class));

		Schema schema = new Schema("my_schema", fields);

		TupleMRBuilder grouper = new TupleMRBuilder(conf);
		grouper.addIntermediateSchema(schema);
		grouper.setGroupByFields("url");
		grouper.setOrderBy(new SortBy().add("url", Order.ASC).add("date", Order.ASC));
		// Input / output and such
		grouper.setTupleReducer(new MovingAverageHandler(nDaysAverage));
		grouper.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		grouper.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new URLVisitsProcessor());
		return grouper.createJob();
	}
	
	private static final String HELP = "Usage: [input_path] [output_path] [num_days_average]";

	public static void main(String args[]) throws TupleMRException, IOException, InterruptedException,
	    ClassNotFoundException {
		if(args.length != 3) {
			System.err.println("Wrong number of arguments");
			System.err.println(HELP);
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		new MovingAverage().getJob(conf, args[0], args[1],Integer.parseInt(args[2])).waitForCompletion(true);
	}
}
